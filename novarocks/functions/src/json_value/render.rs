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

use super::number::{FixedText, number_digits, write_scalar_text};
use super::{
    JsonError, JsonValueError as Error, JsonWork, Kind, MAX_DEPTH, Node, Number, Scalar, Scratch,
};
use crate::KernelFailure;
use std::ops::Range;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TextEvent {
    EnteredItem,
    Byte(u8),
    Done,
}

/// Resumable text from the original carrier. Frames contain offsets rather
/// than references; each advance must receive the same unchanged source.
pub struct TextCursor<R = ()> {
    frames: Scratch<Frame, R>,
    action: Action,
    token: Token,
}

#[derive(Clone, Copy)]
struct Span {
    start: usize,
    end: usize,
}

impl Span {
    fn of(source: &[u8], part: &[u8]) -> Self {
        let start = part.as_ptr().addr() - source.as_ptr().addr();
        Self {
            start,
            end: start + part.len(),
        }
    }

    fn get(self, source: &[u8]) -> Result<&[u8], Error> {
        source.get(self.start..self.end).ok_or(Error::Extent)
    }
}

struct Frame {
    item: Span,
    index: usize,
    consumed: usize,
}

enum Action {
    Start,
    Enter(Span),
    Item(Span),
    Next,
    Key(Span, Span),
    Colon(Span),
}

enum Token {
    Empty,
    Fixed {
        text: FixedText<96>,
        cursor: usize,
    },
    Literal {
        bytes: &'static [u8],
        cursor: usize,
    },
    Number {
        digits: FixedText<200>,
        negative: bool,
        scale: i32,
        cursor: usize,
    },
    Quoted(Quoted),
}

impl<R> Default for TextCursor<R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R> TextCursor<R> {
    pub fn new() -> Self {
        Self {
            frames: Scratch::new(),
            action: Action::Start,
            token: Token::Empty,
        }
    }

    /// Reserves the entire v1 traversal stack in an already-admitted caller,
    /// for callers that need to guarantee no frame allocations during turns.
    pub fn with_preallocated_scratch(
        work: &mut impl JsonWork<Reservation = R>,
    ) -> Result<Self, JsonError> {
        let mut cursor = Self::new();
        let grant = work.reserve(Self::preallocated_scratch_bytes())?;
        cursor
            .frames
            .values
            .try_reserve_exact(MAX_DEPTH)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        cursor.frames.grant = Some(grant);
        Ok(cursor)
    }

    pub const fn preallocated_scratch_bytes() -> usize {
        MAX_DEPTH * size_of::<Frame>()
    }

    pub fn scratch_capacity_bytes(&self) -> usize {
        self.frames.values.capacity() * size_of::<Frame>()
    }

    pub fn reset(&mut self) {
        self.frames.values.clear();
        self.action = Action::Start;
        self.token = Token::Empty;
    }

    /// Largest old+new allocated frame stack during doubling to the v1
    /// document depth. Inline cursor storage is owned by the caller.
    pub const fn peak_scratch_bytes() -> usize {
        (MAX_DEPTH + MAX_DEPTH / 2) * size_of::<Frame>()
    }

    pub fn advance(
        &mut self,
        source: &[u8],
        work: &mut impl JsonWork<Reservation = R>,
    ) -> Result<TextEvent, JsonError> {
        work.step()?;
        if let Some(byte) = self.token.next(source)? {
            return Ok(TextEvent::Byte(byte));
        }
        loop {
            match self.action {
                Action::Start => {
                    let root = Node::from_root(source)?;
                    self.action = Action::Enter(Span::of(source, root.item_bytes()));
                }
                Action::Enter(item) => {
                    self.action = Action::Item(item);
                    return Ok(TextEvent::EnteredItem);
                }
                Action::Item(item) => {
                    for _ in 0..32 {
                        work.step()?;
                    }
                    let node = Node::exact(item.get(source)?)?;
                    self.action = Action::Next;
                    if let Some(bytes) = node.string_bytes()? {
                        self.token = Token::Quoted(Quoted::new(Span::of(source, bytes)));
                    } else {
                        self.token = match node.kind()? {
                            Kind::Null => Token::Literal {
                                bytes: b"null",
                                cursor: 0,
                            },
                            Kind::Scalar(scalar) => Token::scalar(scalar, work)?,
                            kind @ (Kind::Array(_) | Kind::Object(_)) => {
                                if self.frames.values.len() == MAX_DEPTH {
                                    return Err(Error::Depth.into());
                                }
                                self.frames.push(
                                    Frame {
                                        item,
                                        index: 0,
                                        consumed: 0,
                                    },
                                    work,
                                )?;
                                let mark = if matches!(kind, Kind::Object(_)) {
                                    b'{'
                                } else {
                                    b'['
                                };
                                return Ok(TextEvent::Byte(mark));
                            }
                        };
                    }
                }
                Action::Next => {
                    for _ in 0..32 {
                        work.step()?;
                    }
                    let Some(frame) = self.frames.values.last_mut() else {
                        return Ok(TextEvent::Done);
                    };
                    let (container, mark) = match Node::exact(frame.item.get(source)?)?.kind()? {
                        Kind::Array(array) => (array, b']'),
                        Kind::Object(object) => (object, b'}'),
                        _ => return Err(Error::Tag.into()),
                    };
                    match container.raw_next(frame.index, frame.consumed)? {
                        Some((key, node, consumed)) => {
                            let value = Span::of(source, node.item_bytes());
                            self.action = match key {
                                Some(key) => Action::Key(Span::of(source, key), value),
                                None => Action::Enter(value),
                            };
                            let comma = frame.index != 0;
                            frame.index += 1;
                            frame.consumed = consumed;
                            if comma {
                                return Ok(TextEvent::Byte(b','));
                            }
                        }
                        None => {
                            self.frames.values.pop();
                            return Ok(TextEvent::Byte(mark));
                        }
                    }
                }
                Action::Key(key, value) => {
                    self.token = Token::Quoted(Quoted::new(key));
                    self.action = Action::Colon(value);
                }
                Action::Colon(value) => {
                    self.action = Action::Enter(value);
                    return Ok(TextEvent::Byte(b':'));
                }
            }
            if let Some(byte) = self.token.next(source)? {
                return Ok(TextEvent::Byte(byte));
            }
        }
    }
}

impl Token {
    fn scalar(scalar: Scalar<'_>, work: &mut impl JsonWork) -> Result<Self, JsonError> {
        if let Scalar::Number(Number::Finite { scale, coefficient }) = scalar {
            let (digits, negative) = number_digits(coefficient, work)?;
            return Ok(Self::Number {
                digits,
                negative,
                scale,
                cursor: 0,
            });
        }
        let quoted = match scalar {
            Scalar::Date(_) | Scalar::Temporal { .. } | Scalar::Number(_) => true,
            Scalar::Double(value) => !value.is_finite(),
            Scalar::Real(value) => !value.is_finite(),
            _ => false,
        };
        use std::fmt::Write;
        let mut text = FixedText::new();
        if quoted {
            text.write_char('"').map_err(|_| Error::Scalar)?;
        }
        write_scalar_text(&mut text, scalar).map_err(|_| Error::Scalar)?;
        if quoted {
            text.write_char('"').map_err(|_| Error::Scalar)?;
        }
        Ok(Self::Fixed { text, cursor: 0 })
    }

    fn next(&mut self, source: &[u8]) -> Result<Option<u8>, Error> {
        let next = match self {
            Self::Empty => return Ok(None),
            Self::Fixed { text, cursor } => next_literal(text.as_str().as_bytes(), cursor),
            Self::Literal { bytes, cursor } => next_literal(bytes, cursor),
            Self::Number {
                digits,
                negative,
                scale,
                cursor,
            } => {
                let position = *cursor;
                *cursor += 1;
                if *negative && position == 0 {
                    return Ok(Some(b'-'));
                }
                number_byte(
                    digits.as_str().as_bytes(),
                    *scale,
                    position - usize::from(*negative),
                )
            }
            Self::Quoted(quoted) => quoted.next(source)?,
        };
        if next.is_none() {
            *self = Self::Empty;
        }
        Ok(next)
    }
}

fn next_literal(bytes: &[u8], cursor: &mut usize) -> Option<u8> {
    let byte = *bytes.get(*cursor)?;
    *cursor += 1;
    Some(byte)
}

fn number_byte(digits: &[u8], scale: i32, position: usize) -> Option<u8> {
    if scale <= 0 {
        let end = digits.len() + scale.unsigned_abs() as usize;
        return (position < end).then(|| digits.get(position).copied().unwrap_or(b'0'));
    }
    let scale = scale as usize;
    if scale < digits.len() {
        let point = digits.len() - scale;
        return if position == point {
            Some(b'.')
        } else {
            digits
                .get(position - usize::from(position > point))
                .copied()
        };
    }
    match position {
        0 => Some(b'0'),
        1 => Some(b'.'),
        position if position < 2 + scale - digits.len() => Some(b'0'),
        position => digits.get(position - (2 + scale - digits.len())).copied(),
    }
}

struct Quoted {
    remaining: Span,
    queued: [u8; 6],
    queued_bytes: Range<usize>,
    opened: bool,
    closed: bool,
}

impl Quoted {
    fn new(remaining: Span) -> Self {
        Self {
            remaining,
            queued: [0; 6],
            queued_bytes: 0..0,
            opened: false,
            closed: false,
        }
    }

    fn next(&mut self, source: &[u8]) -> Result<Option<u8>, Error> {
        if let Some(at) = self.queued_bytes.next() {
            return Ok(Some(self.queued[at]));
        }
        if !self.opened {
            self.opened = true;
            return Ok(Some(b'"'));
        }
        let Some(&first) = self.remaining.get(source)?.first() else {
            if self.closed {
                return Ok(None);
            }
            self.closed = true;
            return Ok(Some(b'"'));
        };
        let escaped = match first {
            b'"' | b'\\' => [b'\\', first],
            0x08 => *b"\\b",
            0x09 => *b"\\t",
            0x0a => *b"\\n",
            0x0c => *b"\\f",
            0x0d => *b"\\r",
            0x00..=0x1f => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                self.queued = [
                    b'\\',
                    b'u',
                    b'0',
                    b'0',
                    HEX[(first >> 4) as usize],
                    HEX[(first & 15) as usize],
                ];
                self.queued_bytes = 1..6;
                self.remaining.start += 1;
                return Ok(Some(b'\\'));
            }
            0x20..=0x7f => {
                self.remaining.start += 1;
                return Ok(Some(first));
            }
            _ => {
                let width = match first {
                    0xc2..=0xdf => 2,
                    0xe0..=0xef => 3,
                    0xf0..=0xf4 => 4,
                    _ => return Err(Error::Text),
                };
                let bytes = self
                    .remaining
                    .get(source)?
                    .get(..width)
                    .ok_or(Error::Text)?;
                std::str::from_utf8(bytes).map_err(|_| Error::Text)?;
                self.queued[..width].copy_from_slice(bytes);
                self.queued_bytes = 1..width;
                self.remaining.start += width;
                return Ok(Some(first));
            }
        };
        self.queued[..2].copy_from_slice(&escaped);
        self.queued_bytes = 1..2;
        self.remaining.start += 1;
        Ok(Some(escaped[0]))
    }
}

pub fn format(source: &[u8], work: &mut impl JsonWork) -> Result<String, JsonError> {
    let mut size = 0usize;
    let mut cursor = TextCursor::new();
    loop {
        match cursor.advance(source, work)? {
            TextEvent::Byte(_) => {
                size = size
                    .checked_add(1)
                    .ok_or(KernelFailure::ResourceExhausted)?
            }
            TextEvent::EnteredItem => {}
            TextEvent::Done => break,
        }
    }
    cursor.reset();
    work.reserve_output(size)?;
    let mut out = String::new();
    out.try_reserve_exact(size)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    let mut utf8 = [0; 4];
    let mut utf8_len = 0;
    loop {
        match cursor.advance(source, work)? {
            TextEvent::Byte(byte) => {
                utf8[utf8_len] = byte;
                utf8_len += 1;
                match std::str::from_utf8(&utf8[..utf8_len]) {
                    Ok(character) => {
                        out.push_str(character);
                        utf8_len = 0;
                    }
                    Err(error) if error.error_len().is_none() => {}
                    Err(_) => return Err(Error::Text.into()),
                }
            }
            TextEvent::EnteredItem => {}
            TextEvent::Done => break,
        }
    }
    debug_assert_eq!(size, out.len());
    if utf8_len != 0 {
        return Err(Error::Text.into());
    }
    Ok(out)
}
