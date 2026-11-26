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

use super::codec::{
    Layout, NULL_TAG, STRING_TAG, leb_size, scalar_layout, write_leb, write_scalar, write_width,
};
use super::number::Coefficient;
use super::{
    JsonError, JsonValueError, JsonWork, MAX_DEPTH, Node, ObservedError, Scalar, Scratch, VERSION,
};
use crate::KernelFailure;
use crate::cast::array_json::{InvalidJson, LeafError, LexNumber, LexWork, Reader, lex_number};
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug)]
pub enum Input<'a> {
    Null,
    Scalar(Scalar<'a>),
    Array(&'a [Input<'a>]),
    Object(&'a [(&'a str, Input<'a>)]),
    Encoded(Node<'a>),
}

pub(crate) enum Source<'a> {
    Null,
    Scalar(Scalar<'a>),
    StringToken(&'a str, usize),
    InlineString(super::FixedText<96>),
    NumberToken(LexNumber),
    Encoded(Node<'a>),
}

pub(crate) enum Event<'a> {
    Start(bool),
    Key(Source<'a>),
    Item(Source<'a>),
    End,
}

pub(crate) trait Scan<'a, W: JsonWork> {
    fn advance(&mut self, work: &mut W) -> Result<Option<Event<'a>>, JsonError>;
}

pub fn parse(raw: &str, work: &mut impl JsonWork) -> Result<Vec<u8>, JsonError> {
    let layouts = measure(TextScanner::new(raw), work)?;
    emit(TextScanner::new(raw), layouts, work)
}

pub fn encode(value: Input<'_>, work: &mut impl JsonWork) -> Result<Vec<u8>, JsonError> {
    let layouts = measure(InputScanner::new(value), work)?;
    emit(InputScanner::new(value), layouts, work)
}

/// Measure and write resumable events from the same immutable typed input.
pub(crate) fn encode_scan<'a, W: JsonWork, S: Scan<'a, W>>(
    work: &mut W,
    mut scanner: impl FnMut(&mut W) -> Result<S, JsonError>,
) -> Result<Vec<u8>, JsonError> {
    let layout = measure(scanner(work)?, work)?;
    emit(scanner(work)?, layout, work)
}

struct WorkStep<'a, W>(&'a mut W);

impl<W: JsonWork> LexWork for WorkStep<'_, W> {
    fn step(&mut self) -> Result<(), KernelFailure> {
        self.0.step()
    }
    fn flush(&mut self) -> Result<(), KernelFailure> {
        self.0.step()
    }
}

#[derive(Clone, Copy)]
enum Phase {
    FirstArrayItem,
    AnotherArrayItem,
    AfterArrayItem,
    FirstObjectKey,
    AnotherObjectKey,
    ObjectValue,
    AfterObjectValue,
}

struct TextScanner<'a, R> {
    raw: &'a str,
    at: usize,
    root: bool,
    phase: Scratch<Phase, R>,
}

impl<'a, R> TextScanner<'a, R> {
    fn new(raw: &'a str) -> Self {
        Self {
            raw,
            at: 0,
            root: false,
            phase: Scratch::new(),
        }
    }
}

impl<'a, R, W: JsonWork<Reservation = R>> Scan<'a, W> for TextScanner<'a, R> {
    fn advance(&mut self, work: &mut W) -> Result<Option<Event<'a>>, JsonError> {
        let mut adapter = WorkStep(work);
        let mut reader = Reader {
            raw: self.raw,
            at: self.at,
            work: &mut adapter,
        };
        let event = text_step(&mut reader, &mut self.phase.values, &mut self.root)
            .map_err(|error| project_text(error, reader.at))?;
        self.at = reader.at;
        if let Some(Event::Start(object)) = event {
            if self.phase.values.len() == MAX_DEPTH {
                return Err(JsonError::Depth);
            }
            self.phase.push(
                if object {
                    Phase::FirstObjectKey
                } else {
                    Phase::FirstArrayItem
                },
                work,
            )?;
        }
        Ok(event)
    }
}

fn project_text(error: LeafError, offset: usize) -> JsonError {
    match error {
        LeafError::Outer(error) => error.into(),
        LeafError::Parse(
            InvalidJson::Syntax | InvalidJson::NumberRange | InvalidJson::FloatParser(_),
        ) => JsonError::InvalidText(offset),
        #[cfg(test)]
        LeafError::Parse(InvalidJson::NestingDepth) => JsonError::InvalidText(offset),
        #[cfg(test)]
        LeafError::Conversion(_) => JsonValueError::Scalar.into(),
    }
}

fn text_step<'a>(
    reader: &mut Reader<'a, '_>,
    phase: &mut Vec<Phase>,
    root: &mut bool,
) -> Result<Option<Event<'a>>, LeafError> {
    use Phase::*;
    loop {
        reader.space()?;
        let Some(top) = phase.last_mut() else {
            if *root {
                return if reader.byte().is_none() {
                    Ok(None)
                } else {
                    Err(LeafError::Parse(InvalidJson::Syntax))
                };
            }
            *root = true;
            return text_item(reader).map(Some);
        };
        let byte = reader.byte();
        match *top {
            FirstArrayItem | AfterArrayItem if byte == Some(b']') => {
                reader.require(b']')?;
                phase.pop();
                return Ok(Some(Event::End));
            }
            FirstObjectKey | AfterObjectValue if byte == Some(b'}') => {
                reader.require(b'}')?;
                phase.pop();
                return Ok(Some(Event::End));
            }
            FirstArrayItem | AnotherArrayItem => {
                *top = AfterArrayItem;
                return text_item(reader).map(Some);
            }
            AfterArrayItem => {
                reader.require(b',')?;
                *top = AnotherArrayItem;
            }
            FirstObjectKey | AnotherObjectKey => {
                *top = ObjectValue;
                let (range, len) = reader.string_token()?;
                return Ok(Some(Event::Key(Source::StringToken(
                    &reader.raw[range],
                    len,
                ))));
            }
            ObjectValue => {
                reader.require(b':')?;
                reader.space()?;
                *top = AfterObjectValue;
                return text_item(reader).map(Some);
            }
            AfterObjectValue => {
                reader.require(b',')?;
                *top = AnotherObjectKey;
            }
        }
    }
}

fn text_item<'a>(reader: &mut Reader<'a, '_>) -> Result<Event<'a>, LeafError> {
    let source = match reader.byte() {
        Some(b'[') => {
            reader.require(b'[')?;
            return Ok(Event::Start(false));
        }
        Some(b'{') => {
            reader.require(b'{')?;
            return Ok(Event::Start(true));
        }
        Some(b'"') => {
            let (range, len) = reader.string_token()?;
            Source::StringToken(&reader.raw[range], len)
        }
        Some(b'n') => {
            reader.literal(b"null")?;
            Source::Null
        }
        Some(b't') => {
            reader.literal(b"true")?;
            Source::Scalar(Scalar::Boolean(true))
        }
        Some(b'f') => {
            reader.literal(b"false")?;
            Source::Scalar(Scalar::Boolean(false))
        }
        Some(b'-' | b'0'..=b'9') => {
            let range = reader.number_token()?;
            Source::NumberToken(lex_number(&reader.raw[range], reader.work)?)
        }
        _ => return Err(LeafError::Parse(InvalidJson::Syntax)),
    };
    Ok(Event::Item(source))
}

struct InputScanner<'a, R> {
    next: Option<Input<'a>>,
    frames: Scratch<InputFrame<'a>, R>,
}
enum InputFrame<'a> {
    Array(std::slice::Iter<'a, Input<'a>>),
    Object(std::slice::Iter<'a, (&'a str, Input<'a>)>),
}
impl<'a, R> InputScanner<'a, R> {
    fn new(value: Input<'a>) -> Self {
        Self {
            next: Some(value),
            frames: Scratch::new(),
        }
    }
}

impl<'a, R, W: JsonWork<Reservation = R>> Scan<'a, W> for InputScanner<'a, R> {
    fn advance(&mut self, work: &mut W) -> Result<Option<Event<'a>>, JsonError> {
        work.step()?;
        let value = match self.next.take() {
            Some(value) => value,
            None => match self.frames.values.last_mut() {
                None => return Ok(None),
                Some(InputFrame::Array(values)) => match values.next() {
                    Some(value) => *value,
                    None => {
                        self.frames.values.pop();
                        return Ok(Some(Event::End));
                    }
                },
                Some(InputFrame::Object(values)) => match values.next() {
                    Some((key, value)) => {
                        self.next = Some(*value);
                        return Ok(Some(Event::Key(Source::Scalar(Scalar::String(key)))));
                    }
                    None => {
                        self.frames.values.pop();
                        return Ok(Some(Event::End));
                    }
                },
            },
        };
        let (object, frame) = match value {
            Input::Null => return Ok(Some(Event::Item(Source::Null))),
            Input::Scalar(scalar) => return Ok(Some(Event::Item(Source::Scalar(scalar)))),
            Input::Encoded(node) => return Ok(Some(Event::Item(Source::Encoded(node)))),
            Input::Array(values) => (false, InputFrame::Array(values.iter())),
            Input::Object(values) => (true, InputFrame::Object(values.iter())),
        };
        if self.frames.values.len() == MAX_DEPTH {
            return Err(JsonError::Depth);
        }
        self.frames.push(frame, work)?;
        Ok(Some(Event::Start(object)))
    }
}

struct ContainerMeasure {
    object: bool,
    index: usize,
    count: usize,
    members: usize,
}
struct Measured<R> {
    layouts: Scratch<Option<Layout>, R>,
    root_size: usize,
}

fn measure<'a, W: JsonWork>(
    mut reader: impl Scan<'a, W>,
    work: &mut W,
) -> Result<Measured<W::Reservation>, JsonError> {
    let mut layouts = Scratch::new();
    let mut stack = Scratch::<ContainerMeasure, W::Reservation>::new();
    let mut root_size = 0;
    while let Some(event) = reader.advance(work)? {
        work.step()?;
        let (size, item) = match event {
            Event::Start(object) => {
                let index = layouts.values.len();
                layouts.push(None, work)?;
                stack.push(
                    ContainerMeasure {
                        object,
                        index,
                        count: 0,
                        members: 0,
                    },
                    work,
                )?;
                continue;
            }
            Event::End => {
                let frame = stack.values.pop().ok_or(JsonValueError::Extent)?;
                let layout = Layout::new(frame.object, frame.count, frame.members)
                    .map_err(|_| KernelFailure::ResourceExhausted)?;
                layouts.values[frame.index] = Some(layout);
                (layout.size, true)
            }
            Event::Key(source) => (key_size(source)?, false),
            Event::Item(source) => {
                if let Source::Encoded(node) = source {
                    let depth = node.depth(|| work.step()).map_err(|error| match error {
                        ObservedError::Value(cause) => JsonError::Value(cause),
                        ObservedError::Control(cause) => JsonError::Host(cause),
                    })?;
                    if depth + stack.values.len() > MAX_DEPTH {
                        return Err(JsonError::Depth);
                    }
                }
                (item_size(source)?, true)
            }
        };
        if let Some(frame) = stack.values.last_mut() {
            frame.members = checked_add(frame.members, size)?;
            frame.count = checked_add(frame.count, usize::from(item))?;
        } else {
            root_size = size;
        }
    }
    Ok(Measured { layouts, root_size })
}

fn key_size(source: Source<'_>) -> Result<usize, JsonError> {
    let size = match source {
        Source::StringToken(_, len) => len,
        Source::Scalar(Scalar::String(text)) => text.len(),
        Source::InlineString(text) => text.as_str().len(),
        _ => return Err(JsonValueError::Tag.into()),
    };
    Ok(checked_add(leb_size(size), size)?)
}

fn item_size(source: Source<'_>) -> Result<usize, JsonError> {
    Ok(match source {
        Source::Null => 1,
        Source::StringToken(_, len) => checked_add(1 + leb_size(len), len)?,
        Source::InlineString(text) => scalar_layout(Scalar::String(text.as_str()))?.1,
        Source::Scalar(scalar) => scalar_layout(scalar)?.1,
        Source::NumberToken(number) => with_scalar(number, |scalar| Ok(scalar_layout(scalar)?.1))?,
        Source::Encoded(node) => node.item_bytes().len(),
    })
}

fn checked_add(left: usize, right: usize) -> Result<usize, KernelFailure> {
    left.checked_add(right)
        .ok_or(KernelFailure::ResourceExhausted)
}

fn with_scalar<T>(
    number: LexNumber,
    operation: impl FnOnce(Scalar<'_>) -> Result<T, JsonError>,
) -> Result<T, JsonError> {
    let scalar = match number {
        LexNumber::Integer(value) => Scalar::Integer(value),
        LexNumber::BigInt(value) => Scalar::BigInt(value),
        LexNumber::Double(value) => Scalar::Double(value),
        LexNumber::Decimal {
            precision,
            scale,
            coefficient,
        } => Scalar::Decimal {
            precision,
            scale,
            coefficient,
        },
        LexNumber::Number {
            scale,
            negative,
            digits,
            len,
        } => {
            let coefficient = Coefficient::from_digits(&digits[..len], negative)?;
            return operation(Scalar::Number(super::Number::Finite {
                scale,
                coefficient: coefficient.as_bytes(),
            }));
        }
    };
    operation(scalar)
}

#[derive(Clone, Copy)]
struct ContainerWrite {
    layout: Layout,
    offsets: usize,
    ordinals: usize,
    members: usize,
    count: usize,
}

fn emit<'a, W: JsonWork>(
    mut reader: impl Scan<'a, W>,
    measured: Measured<W::Reservation>,
    work: &mut W,
) -> Result<Vec<u8>, JsonError> {
    let size = checked_add(1, measured.root_size)?;
    work.reserve_output(size)?;
    let mut out = Vec::new();
    out.try_reserve_exact(size)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    while out.len() < size {
        work.step()?;
        out.resize(size.min(out.len().saturating_add(4096)), 0);
    }
    out[0] = VERSION;
    let mut stack = Scratch::<ContainerWrite, W::Reservation>::new();
    let mut layouts = measured.layouts.values.iter();
    let mut at = 1;
    let mut has_key = false;
    while let Some(event) = reader.advance(work)? {
        work.step()?;
        if matches!(event, Event::Start(_) | Event::Key(_) | Event::Item(_)) {
            if !has_key && let Some(parent) = stack.values.last_mut() {
                parent.record_offset(&mut out, at);
            }
            has_key = matches!(event, Event::Key(_));
        }
        match event {
            Event::Start(_) => {
                let layout = layouts
                    .next()
                    .copied()
                    .flatten()
                    .ok_or(JsonValueError::Extent)?;
                let (offsets, ordinals, members) = layout.write_header(&mut out[at..]);
                stack.push(
                    ContainerWrite {
                        layout,
                        offsets: at + offsets,
                        ordinals: at + ordinals,
                        members: at + members,
                        count: 0,
                    },
                    work,
                )?;
                at += members;
            }
            Event::End => {
                let mut frame = stack.values.pop().ok_or(JsonValueError::Extent)?;
                debug_assert_eq!(frame.count, frame.layout.count);
                frame.record_offset(&mut out, at);
                frame.sort_object_directory(&mut out, work)?;
            }
            Event::Key(source) => at += write_key(source, &mut out[at..], work)?,
            Event::Item(source) => at += write_item(source, &mut out[at..], work)?,
        }
    }
    debug_assert_eq!(at, out.len());
    Ok(out)
}

fn write_key(
    source: Source<'_>,
    dst: &mut [u8],
    work: &mut impl JsonWork,
) -> Result<usize, JsonError> {
    Ok(match source {
        Source::InlineString(text) => {
            write_key(Source::Scalar(Scalar::String(text.as_str())), dst, work)?
        }
        Source::StringToken(token, len) => write_token(token, len, dst, work)?,
        Source::Scalar(Scalar::String(text)) => {
            let start = write_leb(dst, text.len());
            copy_observed(&mut dst[start..], text.as_bytes(), work)?;
            start + text.len()
        }
        _ => return Err(JsonValueError::Tag.into()),
    })
}

fn write_item(
    source: Source<'_>,
    dst: &mut [u8],
    work: &mut impl JsonWork,
) -> Result<usize, JsonError> {
    Ok(match source {
        Source::Null => {
            dst[0] = NULL_TAG;
            1
        }
        Source::Scalar(Scalar::String(text)) => {
            dst[0] = STRING_TAG;
            1 + write_key(Source::Scalar(Scalar::String(text)), &mut dst[1..], work)?
        }
        Source::Scalar(value) => write_scalar(value, dst)?,
        Source::InlineString(text) => {
            write_item(Source::Scalar(Scalar::String(text.as_str())), dst, work)?
        }
        Source::StringToken(token, len) => {
            dst[0] = STRING_TAG;
            1 + write_token(token, len, &mut dst[1..], work)?
        }
        Source::NumberToken(number) => {
            with_scalar(number, |scalar| Ok(write_scalar(scalar, dst)?))?
        }
        Source::Encoded(node) => {
            let item = node.item_bytes();
            copy_observed(dst, item, work)?;
            item.len()
        }
    })
}

fn copy_observed(dst: &mut [u8], src: &[u8], work: &mut impl JsonWork) -> Result<(), JsonError> {
    for (dst, src) in dst.chunks_mut(256).zip(src.chunks(256)) {
        work.step()?;
        dst[..src.len()].copy_from_slice(src);
    }
    Ok(())
}

fn write_token(
    token: &str,
    len: usize,
    dst: &mut [u8],
    work: &mut impl JsonWork,
) -> Result<usize, JsonError> {
    let start = write_leb(dst, len);
    let mut at = start;
    let mut adapter = WorkStep(work);
    let mut cursor = Reader {
        raw: token,
        at: 1,
        work: &mut adapter,
    };
    let mut unicode = [0; 4];
    while cursor.byte() != Some(b'"') {
        let ch = cursor
            .string_char()
            .map_err(|cause| project_text(cause, cursor.at))?;
        let bytes = ch.encode_utf8(&mut unicode).as_bytes();
        dst[at..at + bytes.len()].copy_from_slice(bytes);
        at += bytes.len();
    }
    debug_assert_eq!(at, start + len);
    Ok(at)
}

impl ContainerWrite {
    fn record_offset(&mut self, out: &mut [u8], at: usize) {
        if self.layout.offset_width != 0 {
            write_width(
                &mut out[self.offsets..],
                self.layout.offset_width,
                self.count,
                at - self.members,
            );
        }
        self.count += 1;
    }

    fn sort_object_directory(
        &self,
        out: &mut [u8],
        work: &mut impl JsonWork,
    ) -> Result<(), JsonError> {
        let width = self.layout.ordinal_width;
        if width == 0 {
            return Ok(());
        }
        let len = self.layout.count;
        let (header, members) = out.split_at_mut(self.members);
        let (offsets, ordinals) = header[self.offsets..].split_at_mut(self.ordinals - self.offsets);
        for i in 0..len {
            work.step()?;
            write_width(ordinals, width, i, i);
        }
        let mut heap = OrdinalHeap {
            offsets,
            ordinals,
            members,
            offset_width: self.layout.offset_width,
            width,
        };
        for start in (0..len / 2).rev() {
            heap.sift(start, len, work)?;
        }
        for end in (1..len).rev() {
            heap.swap(0, end);
            heap.sift(0, end, work)?;
        }
        Ok(())
    }
}

struct OrdinalHeap<'a> {
    offsets: &'a [u8],
    ordinals: &'a mut [u8],
    members: &'a [u8],
    offset_width: usize,
    width: usize,
}
impl OrdinalHeap<'_> {
    fn compare(
        &self,
        left_index: usize,
        right_index: usize,
        work: &mut impl JsonWork,
    ) -> Result<Ordering, JsonError> {
        let left = super::codec::read_width(self.ordinals, self.width, left_index)?;
        let right = super::codec::read_width(self.ordinals, self.width, right_index)?;
        let key = |ordinal| -> Result<&[u8], JsonValueError> {
            let offset = super::codec::read_width(self.offsets, self.offset_width, ordinal)?;
            super::codec::Reader(&self.members[offset..]).blob()
        };
        let left_key = key(left)?;
        let right_key = key(right)?;
        work.step()?;
        for (a, b) in left_key.chunks(256).zip(right_key.chunks(256)) {
            work.step()?;
            let order = a.cmp(b);
            if !order.is_eq() {
                return Ok(order);
            }
        }
        Ok(left_key
            .len()
            .cmp(&right_key.len())
            .then_with(|| left.cmp(&right)))
    }
    fn swap(&mut self, left: usize, right: usize) {
        for i in 0..self.width {
            self.ordinals
                .swap(left * self.width + i, right * self.width + i);
        }
    }
    fn sift(
        &mut self,
        mut parent: usize,
        end: usize,
        work: &mut impl JsonWork,
    ) -> Result<(), JsonError> {
        while parent < end / 2 {
            let left = parent * 2 + 1;
            let right = left + 1;
            let larger = if right < end && self.compare(left, right, work)?.is_lt() {
                right
            } else {
                left
            };
            if !self.compare(parent, larger, work)?.is_lt() {
                break;
            }
            self.swap(parent, larger);
            parent = larger;
        }
        Ok(())
    }
}
