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

//! Push-based, chunk-fed JSON scanner for minimal views.
//!
//! The JSON parsers in the dependency graph are pull parsers: they need the
//! whole input or a blocking `Read`. A mutation confirmation must instead be
//! decoded in the caller's task, one received chunk at a time, while holding
//! only what the minimal view needs. This scanner validates the complete
//! RFC 8259 grammar (escapes, surrogate pairs, UTF-8, numbers, literals,
//! nesting, trailing content) and reports values only at the paths its
//! [`ViewSink`] asks for. Everything else is validated byte by byte and
//! dropped without being buffered.
//!
//! Retention is bounded independently of the input: one scratch buffer sized
//! by the sink's largest retained value (at least [`MAX_KEY_LEN`]) and one
//! frame stack sized by [`MAX_DEPTH`], both allocated up front. Chunk
//! boundaries may fall anywhere, including inside strings, escapes, numbers
//! and multi-byte UTF-8 sequences.

use std::fmt;

/// Physical JSON container depth, distinct from the logical value/type depth.
/// A table response adds root/metadata/schemas/schema/fields/field (six
/// containers) before a top-level type. Each further STRUCT adds type/fields/
/// field (three); LIST/MAP and their defaults cannot exceed that path.
/// Streaming confirmation views use this same complete table-response grammar.
pub(crate) const MAX_DEPTH: usize = 6 + 3 * (novarocks_type_contract::MAX_VALUE_TYPE_DEPTH - 1);

/// Longest object key compared against the sink's paths. Longer keys inside
/// an interested object are reported as [`Key::Overlong`].
pub(crate) const MAX_KEY_LEN: usize = 1024;

/// Why a document was not accepted.
///
/// Carries only byte offsets and NovaRocks-owned field names: never response
/// text, which may carry credentials.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScanError {
    /// A byte that the grammar does not allow at this position.
    Syntax { offset: u64 },
    /// A malformed escape sequence, including unpaired surrogates.
    InvalidEscape { offset: u64 },
    /// A malformed UTF-8 sequence inside a string.
    InvalidUtf8 { offset: u64 },
    /// An unescaped control character inside a string.
    ControlCharacter { offset: u64 },
    /// Content after the complete top-level value.
    TrailingContent { offset: u64 },
    /// Containers nested deeper than [`MAX_DEPTH`].
    NestingTooDeep { offset: u64, limit: usize },
    /// The input ended before one complete top-level value.
    Truncated,
    /// A field the view uses occurred more than once.
    DuplicateField(&'static str),
    /// A field the view uses has the wrong JSON type.
    UnexpectedType(&'static str),
    /// A retained value exceeded its length limit.
    ValueTooLong(&'static str),
    /// A retained collection exceeded its entry limit.
    TooManyEntries(&'static str),
    /// A retained value is well-formed JSON but not a valid value.
    InvalidValue(&'static str),
    /// A field the view requires is absent.
    MissingField(&'static str),
}

impl fmt::Display for ScanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Syntax { offset } => write!(f, "JSON syntax error at byte {offset}"),
            Self::InvalidEscape { offset } => write!(f, "invalid JSON escape at byte {offset}"),
            Self::InvalidUtf8 { offset } => {
                write!(f, "invalid UTF-8 in JSON string at byte {offset}")
            }
            Self::ControlCharacter { offset } => {
                write!(
                    f,
                    "unescaped control character in JSON string at byte {offset}"
                )
            }
            Self::TrailingContent { offset } => {
                write!(f, "trailing content after JSON document at byte {offset}")
            }
            Self::NestingTooDeep { offset, limit } => {
                write!(f, "JSON nesting exceeds {limit} levels at byte {offset}")
            }
            Self::Truncated => f.write_str("JSON document is truncated"),
            Self::DuplicateField(field) => write!(f, "JSON field `{field}` occurs more than once"),
            Self::UnexpectedType(field) => write!(f, "JSON field `{field}` has an unexpected type"),
            Self::ValueTooLong(field) => write!(f, "JSON field `{field}` exceeds its length limit"),
            Self::TooManyEntries(field) => {
                write!(f, "JSON field `{field}` exceeds its entry limit")
            }
            Self::InvalidValue(field) => write!(f, "JSON field `{field}` has an invalid value"),
            Self::MissingField(field) => write!(f, "JSON field `{field}` is missing"),
        }
    }
}

impl std::error::Error for ScanError {}

/// The JSON type of a value, known from its first byte.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ValueKind {
    Object,
    Array,
    String,
    Number,
    Bool,
    Null,
}

/// A decoded object key inside an interested object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Key<'a> {
    Text(&'a str),
    /// Longer than [`MAX_KEY_LEN`]; validated but not retained.
    Overlong,
}

/// The source text of a JSON number, which the grammar already validated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NumberText<'a> {
    text: &'a str,
    integer: bool,
}

impl<'a> NumberText<'a> {
    pub(crate) fn text(&self) -> &'a str {
        self.text
    }

    /// The value as an `i64`, when it has neither fraction nor exponent and
    /// fits.
    pub(crate) fn as_i64(&self) -> Option<i64> {
        if self.integer {
            self.text.parse().ok()
        } else {
            None
        }
    }
}

/// A complete scalar at an interested node.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Scalar<'a> {
    Str(&'a str),
    Number(NumberText<'a>),
    Bool(bool),
    Null,
    /// A string or number longer than the limit the sink gave in
    /// [`ViewSink::open`]; validated but not retained.
    Overlong(ValueKind),
}

/// The consumer of one scan: it chooses the paths it needs and builds the view.
///
/// Nodes are sink-defined path positions. A `None` answer from [`member`] or
/// [`element`] skips that whole subtree without any further callback.
///
/// [`member`]: ViewSink::member
/// [`element`]: ViewSink::element
pub(crate) trait ViewSink {
    type Node: Copy;
    type View;

    /// The longest string or number text this sink ever retains. It sizes
    /// the scanner's scratch buffer once.
    fn retention_cap(&self) -> usize;

    /// The node of the top-level value.
    fn root(&self) -> Self::Node;

    /// A value at `node` begins. Rejects the wrong kind; for strings and
    /// numbers returns the retention limit (clamped to
    /// [`retention_cap`](ViewSink::retention_cap)).
    fn open(&mut self, node: Self::Node, kind: ValueKind) -> Result<usize, ScanError>;

    /// A member key of the interested object `object` was decoded.
    fn member(&mut self, object: Self::Node, key: Key<'_>)
    -> Result<Option<Self::Node>, ScanError>;

    /// Element `index` of the interested array `array` begins.
    fn element(&mut self, array: Self::Node, index: u64) -> Result<Option<Self::Node>, ScanError>;

    /// A scalar at `node` completed.
    fn scalar(&mut self, node: Self::Node, value: Scalar<'_>) -> Result<(), ScanError>;

    /// The container at `node` closed.
    fn close(&mut self, node: Self::Node) -> Result<(), ScanError>;

    /// The document is complete and valid; build the view.
    fn finish(self) -> Result<Self::View, ScanError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Container {
    Object,
    Array,
}

#[derive(Clone, Copy, Debug)]
struct Frame<N> {
    container: Container,
    node: Option<N>,
    next_index: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Expect {
    Value,
    FirstElementOrEnd,
    FirstKeyOrEnd,
    Key,
    Colon,
    CommaOrEnd,
    Done,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Escape {
    None,
    Backslash,
    Hex {
        high: Option<u16>,
        digits: u8,
        value: u16,
    },
    /// A high surrogate was decoded; `\` must follow.
    LowBackslash(u16),
    /// A high surrogate and `\` were decoded; `u` must follow.
    LowU(u16),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StrLex {
    key: bool,
    escape: Escape,
    utf8_remaining: u8,
    utf8_low: u8,
    utf8_high: u8,
}

impl StrLex {
    fn new(key: bool) -> Self {
        Self {
            key,
            escape: Escape::None,
            utf8_remaining: 0,
            utf8_low: 0x80,
            utf8_high: 0xBF,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NumState {
    Minus,
    Zero,
    Int,
    Dot,
    Frac,
    Exp,
    ExpSign,
    ExpInt,
}

impl NumState {
    fn is_terminal(self) -> bool {
        matches!(self, Self::Zero | Self::Int | Self::Frac | Self::ExpInt)
    }

    fn step(self, byte: u8) -> Option<Self> {
        match (self, byte) {
            (Self::Minus, b'0') => Some(Self::Zero),
            (Self::Minus, b'1'..=b'9') => Some(Self::Int),
            (Self::Zero | Self::Int, b'.') => Some(Self::Dot),
            (Self::Zero | Self::Int | Self::Frac, b'e' | b'E') => Some(Self::Exp),
            (Self::Int, b'0'..=b'9') => Some(Self::Int),
            (Self::Dot | Self::Frac, b'0'..=b'9') => Some(Self::Frac),
            (Self::Exp, b'+' | b'-') => Some(Self::ExpSign),
            (Self::Exp | Self::ExpSign | Self::ExpInt, b'0'..=b'9') => Some(Self::ExpInt),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LitLex {
    text: &'static [u8],
    matched: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
    Between(Expect),
    Str(StrLex),
    Num(NumState),
    Lit(LitLex),
}

/// A chunk-fed scanner that validates one JSON document and drives a sink.
pub(crate) struct JsonScanner<S: ViewSink> {
    sink: S,
    frames: Vec<Frame<S::Node>>,
    max_depth: usize,
    mode: Mode,
    /// Node of the member value that follows the current key.
    member_node: Option<S::Node>,
    /// Node of the scalar being lexed; `None` when skipped.
    scalar_node: Option<S::Node>,
    /// Retention limit of the token being lexed; `None` validates only.
    retain: Option<usize>,
    overflow: bool,
    scratch: Vec<u8>,
    scratch_cap: usize,
    consumed: u64,
    failed: Option<ScanError>,
}

impl<S: ViewSink> JsonScanner<S> {
    pub(crate) fn new(sink: S) -> Self {
        Self::with_max_depth(sink, MAX_DEPTH)
    }

    fn with_max_depth(sink: S, max_depth: usize) -> Self {
        let scratch_cap = sink.retention_cap().max(MAX_KEY_LEN);
        Self {
            sink,
            frames: Vec::with_capacity(max_depth),
            max_depth,
            mode: Mode::Between(Expect::Value),
            member_node: None,
            scalar_node: None,
            retain: None,
            overflow: false,
            scratch: Vec::with_capacity(scratch_cap),
            scratch_cap,
            consumed: 0,
            failed: None,
        }
    }

    /// Bytes the scanner itself holds: the scratch buffer and the frame
    /// stack. Both are allocated once and never grow.
    pub(crate) fn retained_bytes(&self) -> usize {
        self.scratch.capacity() + self.frames.capacity() * std::mem::size_of::<Frame<S::Node>>()
    }

    /// Bytes fed so far.
    pub(crate) fn consumed(&self) -> u64 {
        self.consumed
    }

    /// Process one chunk. After an error every later call returns that error.
    pub(crate) fn feed(&mut self, chunk: &[u8]) -> Result<(), ScanError> {
        if let Some(error) = self.failed {
            return Err(error);
        }
        let result = self.feed_inner(chunk);
        if let Err(error) = result {
            self.failed = Some(error);
        }
        result
    }

    /// The input ended: require exactly one complete value and build the view.
    pub(crate) fn finish(mut self) -> Result<S::View, ScanError> {
        if let Some(error) = self.failed {
            return Err(error);
        }
        // A top-level number has no closing delimiter of its own.
        if let Mode::Num(state) = self.mode
            && self.frames.is_empty()
            && state.is_terminal()
        {
            self.complete_number(state)?;
        }
        match self.mode {
            Mode::Between(Expect::Done) => self.sink.finish(),
            _ => Err(ScanError::Truncated),
        }
    }

    fn feed_inner(&mut self, chunk: &[u8]) -> Result<(), ScanError> {
        let mut i = 0;
        while i < chunk.len() {
            match self.mode {
                Mode::Between(expect) => {
                    let byte = chunk[i];
                    let offset = self.offset(i);
                    i += 1;
                    if !matches!(byte, b' ' | b'\t' | b'\n' | b'\r') {
                        self.structural(expect, byte, offset)?;
                    }
                }
                Mode::Str(lex) => i = self.string(lex, chunk, i)?,
                Mode::Num(state) => i = self.number(state, chunk, i)?,
                Mode::Lit(lit) => i = self.literal(lit, chunk, i)?,
            }
        }
        self.consumed += chunk.len() as u64;
        Ok(())
    }

    fn offset(&self, index: usize) -> u64 {
        self.consumed + index as u64
    }

    fn structural(&mut self, expect: Expect, byte: u8, offset: u64) -> Result<(), ScanError> {
        match expect {
            Expect::Done => Err(ScanError::TrailingContent { offset }),
            Expect::Value => self.begin_value(byte, offset),
            Expect::FirstElementOrEnd if byte == b']' => self.close_container(),
            Expect::FirstElementOrEnd => self.begin_value(byte, offset),
            Expect::FirstKeyOrEnd if byte == b'}' => self.close_container(),
            Expect::FirstKeyOrEnd | Expect::Key if byte == b'"' => {
                self.begin_key();
                Ok(())
            }
            Expect::Colon if byte == b':' => {
                self.mode = Mode::Between(Expect::Value);
                Ok(())
            }
            Expect::CommaOrEnd => {
                let container = self.frames.last().map(|frame| frame.container);
                match (container, byte) {
                    (Some(Container::Object), b',') => {
                        self.mode = Mode::Between(Expect::Key);
                        Ok(())
                    }
                    (Some(Container::Array), b',') => {
                        self.mode = Mode::Between(Expect::Value);
                        Ok(())
                    }
                    (Some(Container::Object), b'}') | (Some(Container::Array), b']') => {
                        self.close_container()
                    }
                    _ => Err(ScanError::Syntax { offset }),
                }
            }
            Expect::FirstKeyOrEnd | Expect::Key | Expect::Colon => {
                Err(ScanError::Syntax { offset })
            }
        }
    }

    /// The node of the value that starts now.
    fn value_node(&mut self) -> Result<Option<S::Node>, ScanError> {
        let Some(frame) = self.frames.last_mut() else {
            return Ok(Some(self.sink.root()));
        };
        match frame.container {
            Container::Object => Ok(self.member_node.take()),
            Container::Array => {
                let index = frame.next_index;
                frame.next_index += 1;
                match frame.node {
                    Some(parent) => self.sink.element(parent, index),
                    None => Ok(None),
                }
            }
        }
    }

    fn begin_value(&mut self, byte: u8, offset: u64) -> Result<(), ScanError> {
        let node = self.value_node()?;
        let (container, kind, next) = match byte {
            b'{' => (Container::Object, ValueKind::Object, Expect::FirstKeyOrEnd),
            b'[' => (
                Container::Array,
                ValueKind::Array,
                Expect::FirstElementOrEnd,
            ),
            b'"' => {
                self.begin_scalar(node, ValueKind::String)?;
                self.mode = Mode::Str(StrLex::new(false));
                return Ok(());
            }
            b'-' | b'0'..=b'9' => {
                self.begin_scalar(node, ValueKind::Number)?;
                self.retain_bytes(&[byte]);
                self.mode = Mode::Num(match byte {
                    b'-' => NumState::Minus,
                    b'0' => NumState::Zero,
                    _ => NumState::Int,
                });
                return Ok(());
            }
            b't' | b'f' | b'n' => {
                let (text, kind): (&'static [u8], _) = match byte {
                    b't' => (b"true", ValueKind::Bool),
                    b'f' => (b"false", ValueKind::Bool),
                    _ => (b"null", ValueKind::Null),
                };
                self.begin_scalar(node, kind)?;
                self.mode = Mode::Lit(LitLex { text, matched: 1 });
                return Ok(());
            }
            _ => return Err(ScanError::Syntax { offset }),
        };
        if self.frames.len() >= self.max_depth {
            return Err(ScanError::NestingTooDeep {
                offset,
                limit: self.max_depth,
            });
        }
        if let Some(node) = node {
            self.sink.open(node, kind)?;
        }
        self.frames.push(Frame {
            container,
            node,
            next_index: 0,
        });
        self.mode = Mode::Between(next);
        Ok(())
    }

    fn begin_scalar(&mut self, node: Option<S::Node>, kind: ValueKind) -> Result<(), ScanError> {
        let retain = match node {
            Some(node) => Some(self.sink.open(node, kind)?.min(self.scratch_cap)),
            None => None,
        };
        self.scalar_node = node;
        self.begin_token(retain);
        Ok(())
    }

    fn begin_key(&mut self) {
        let interested = self.frames.last().is_some_and(|frame| frame.node.is_some());
        self.begin_token(interested.then_some(MAX_KEY_LEN));
        self.mode = Mode::Str(StrLex::new(true));
    }

    fn begin_token(&mut self, retain: Option<usize>) {
        self.retain = retain;
        self.overflow = false;
        self.scratch.clear();
    }

    fn retain_bytes(&mut self, bytes: &[u8]) {
        let Some(limit) = self.retain else {
            return;
        };
        if self.overflow {
            return;
        }
        if self.scratch.len() + bytes.len() > limit {
            // Never grow past the preallocated cap; the rest of the token is
            // validated only.
            self.overflow = true;
        } else {
            self.scratch.extend_from_slice(bytes);
        }
    }

    fn retain_char(&mut self, value: u32, offset: u64) -> Result<(), ScanError> {
        let ch = char::from_u32(value).ok_or(ScanError::InvalidEscape { offset })?;
        let mut encoded = [0_u8; 4];
        self.retain_bytes(ch.encode_utf8(&mut encoded).as_bytes());
        Ok(())
    }

    fn string(&mut self, mut lex: StrLex, chunk: &[u8], mut i: usize) -> Result<usize, ScanError> {
        while i < chunk.len() {
            let byte = chunk[i];
            let offset = self.offset(i);
            if lex.utf8_remaining > 0 {
                if byte < lex.utf8_low || byte > lex.utf8_high {
                    return Err(ScanError::InvalidUtf8 { offset });
                }
                self.retain_bytes(&[byte]);
                lex.utf8_remaining -= 1;
                lex.utf8_low = 0x80;
                lex.utf8_high = 0xBF;
                i += 1;
                continue;
            }
            match lex.escape {
                Escape::None => {
                    let run = chunk[i..]
                        .iter()
                        .position(|&b| !is_plain_string_byte(b))
                        .unwrap_or(chunk.len() - i);
                    if run > 0 {
                        self.retain_bytes(&chunk[i..i + run]);
                        i += run;
                        continue;
                    }
                    i += 1;
                    match byte {
                        b'"' => {
                            self.complete_string(lex.key)?;
                            return Ok(i);
                        }
                        b'\\' => lex.escape = Escape::Backslash,
                        0x00..=0x1F => return Err(ScanError::ControlCharacter { offset }),
                        _ => {
                            let (remaining, low, high) =
                                utf8_lead(byte).ok_or(ScanError::InvalidUtf8 { offset })?;
                            self.retain_bytes(&[byte]);
                            lex.utf8_remaining = remaining;
                            lex.utf8_low = low;
                            lex.utf8_high = high;
                        }
                    }
                }
                Escape::Backslash => {
                    i += 1;
                    let decoded = match byte {
                        b'"' => b'"',
                        b'\\' => b'\\',
                        b'/' => b'/',
                        b'b' => 0x08,
                        b'f' => 0x0C,
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'u' => {
                            lex.escape = Escape::Hex {
                                high: None,
                                digits: 0,
                                value: 0,
                            };
                            continue;
                        }
                        _ => return Err(ScanError::InvalidEscape { offset }),
                    };
                    self.retain_bytes(&[decoded]);
                    lex.escape = Escape::None;
                }
                Escape::Hex {
                    high,
                    digits,
                    value,
                } => {
                    i += 1;
                    let digit = hex_value(byte).ok_or(ScanError::InvalidEscape { offset })?;
                    let value = (value << 4) | u16::from(digit);
                    if digits < 3 {
                        lex.escape = Escape::Hex {
                            high,
                            digits: digits + 1,
                            value,
                        };
                        continue;
                    }
                    lex.escape = match (high, value) {
                        (None, 0xD800..=0xDBFF) => Escape::LowBackslash(value),
                        (None, 0xDC00..=0xDFFF) => return Err(ScanError::InvalidEscape { offset }),
                        (None, _) => {
                            self.retain_char(u32::from(value), offset)?;
                            Escape::None
                        }
                        (Some(high), 0xDC00..=0xDFFF) => {
                            let code = 0x10000
                                + ((u32::from(high) - 0xD800) << 10)
                                + (u32::from(value) - 0xDC00);
                            self.retain_char(code, offset)?;
                            Escape::None
                        }
                        (Some(_), _) => return Err(ScanError::InvalidEscape { offset }),
                    };
                }
                Escape::LowBackslash(high) => {
                    i += 1;
                    if byte != b'\\' {
                        return Err(ScanError::InvalidEscape { offset });
                    }
                    lex.escape = Escape::LowU(high);
                }
                Escape::LowU(high) => {
                    i += 1;
                    if byte != b'u' {
                        return Err(ScanError::InvalidEscape { offset });
                    }
                    lex.escape = Escape::Hex {
                        high: Some(high),
                        digits: 0,
                        value: 0,
                    };
                }
            }
        }
        self.mode = Mode::Str(lex);
        Ok(i)
    }

    fn complete_string(&mut self, key: bool) -> Result<(), ScanError> {
        if key {
            let parent = self.frames.last().and_then(|frame| frame.node);
            self.member_node = match parent {
                Some(parent) => {
                    let key = if self.overflow {
                        Key::Overlong
                    } else {
                        Key::Text(retained_text(&self.scratch, self.consumed)?)
                    };
                    self.sink.member(parent, key)?
                }
                None => None,
            };
            self.mode = Mode::Between(Expect::Colon);
            return Ok(());
        }
        if let Some(node) = self.scalar_node.take() {
            if self.overflow {
                self.sink
                    .scalar(node, Scalar::Overlong(ValueKind::String))?;
            } else {
                let text = retained_text(&self.scratch, self.consumed)?;
                self.sink.scalar(node, Scalar::Str(text))?;
            }
        }
        self.after_value();
        Ok(())
    }

    fn number(
        &mut self,
        mut state: NumState,
        chunk: &[u8],
        mut i: usize,
    ) -> Result<usize, ScanError> {
        while i < chunk.len() {
            let byte = chunk[i];
            match state.step(byte) {
                Some(next) => {
                    state = next;
                    self.retain_bytes(&[byte]);
                    i += 1;
                }
                // The delimiter is not consumed: it is the next structural byte.
                None if state.is_terminal() => {
                    self.complete_number(state)?;
                    return Ok(i);
                }
                None => {
                    return Err(ScanError::Syntax {
                        offset: self.offset(i),
                    });
                }
            }
        }
        self.mode = Mode::Num(state);
        Ok(i)
    }

    fn complete_number(&mut self, state: NumState) -> Result<(), ScanError> {
        if let Some(node) = self.scalar_node.take() {
            if self.overflow {
                self.sink
                    .scalar(node, Scalar::Overlong(ValueKind::Number))?;
            } else {
                let integer = matches!(state, NumState::Zero | NumState::Int);
                let text = retained_text(&self.scratch, self.consumed)?;
                self.sink
                    .scalar(node, Scalar::Number(NumberText { text, integer }))?;
            }
        }
        self.after_value();
        Ok(())
    }

    fn literal(&mut self, mut lit: LitLex, chunk: &[u8], mut i: usize) -> Result<usize, ScanError> {
        while i < chunk.len() {
            if chunk[i] != lit.text[lit.matched] {
                return Err(ScanError::Syntax {
                    offset: self.offset(i),
                });
            }
            lit.matched += 1;
            i += 1;
            if lit.matched == lit.text.len() {
                if let Some(node) = self.scalar_node.take() {
                    let value = match lit.text {
                        b"true" => Scalar::Bool(true),
                        b"false" => Scalar::Bool(false),
                        _ => Scalar::Null,
                    };
                    self.sink.scalar(node, value)?;
                }
                self.after_value();
                return Ok(i);
            }
        }
        self.mode = Mode::Lit(lit);
        Ok(i)
    }

    fn close_container(&mut self) -> Result<(), ScanError> {
        if let Some(Frame {
            node: Some(node), ..
        }) = self.frames.pop()
        {
            self.sink.close(node)?;
        }
        self.after_value();
        Ok(())
    }

    fn after_value(&mut self) {
        self.mode = Mode::Between(if self.frames.is_empty() {
            Expect::Done
        } else {
            Expect::CommaOrEnd
        });
    }
}

/// The lexer only retains validated UTF-8, so this cannot fail; it is checked
/// rather than assumed.
fn retained_text(scratch: &[u8], consumed: u64) -> Result<&str, ScanError> {
    std::str::from_utf8(scratch).map_err(|_| ScanError::InvalidUtf8 { offset: consumed })
}

fn is_plain_string_byte(byte: u8) -> bool {
    (0x20..0x80).contains(&byte) && byte != b'"' && byte != b'\\'
}

/// For a UTF-8 lead byte: continuation count and the allowed range of the
/// first continuation byte (rejecting overlong forms, surrogates and code
/// points above U+10FFFF).
fn utf8_lead(byte: u8) -> Option<(u8, u8, u8)> {
    match byte {
        0xC2..=0xDF => Some((1, 0x80, 0xBF)),
        0xE0 => Some((2, 0xA0, 0xBF)),
        0xE1..=0xEC | 0xEE..=0xEF => Some((2, 0x80, 0xBF)),
        0xED => Some((2, 0x80, 0x9F)),
        0xF0 => Some((3, 0x90, 0xBF)),
        0xF1..=0xF3 => Some((3, 0x80, 0xBF)),
        0xF4 => Some((3, 0x80, 0x8F)),
        _ => None,
    }
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Scan one complete in-memory document.
pub(crate) fn scan_complete<S: ViewSink>(sink: S, document: &[u8]) -> Result<S::View, ScanError> {
    let mut scanner = JsonScanner::new(sink);
    scanner.feed(document)?;
    scanner.finish()
}

/// Validate one complete document without constructing a retained JSON value.
/// Uses the same grammar and finite physical-depth bound as streamed REST views.
pub(crate) fn validate_document_with_depth(
    document: &[u8],
    max_depth: usize,
) -> Result<(), ScanError> {
    let mut scanner = JsonScanner::with_max_depth(SkippingSink, max_depth);
    scanner.feed(document)?;
    scanner.finish()
}

#[cfg(test)]
pub(crate) fn validate_document(document: &[u8]) -> Result<(), ScanError> {
    validate_document_with_depth(document, MAX_DEPTH)
}

/// Validates every byte while retaining no document values.
struct SkippingSink;

impl ViewSink for SkippingSink {
    type Node = ();
    type View = ();

    fn retention_cap(&self) -> usize {
        0
    }
    fn root(&self) {}
    fn open(&mut self, _node: (), _kind: ValueKind) -> Result<usize, ScanError> {
        Ok(0)
    }
    fn member(&mut self, _object: (), _key: Key<'_>) -> Result<Option<()>, ScanError> {
        Ok(None)
    }
    fn element(&mut self, _array: (), _index: u64) -> Result<Option<()>, ScanError> {
        Ok(None)
    }
    fn scalar(&mut self, _node: (), _value: Scalar<'_>) -> Result<(), ScanError> {
        Ok(())
    }
    fn close(&mut self, _node: ()) -> Result<(), ScanError> {
        Ok(())
    }
    fn finish(self) -> Result<(), ScanError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Interested in every path; records every callback so that split
    /// patterns can be compared event by event.
    struct RecordingSink {
        limit: usize,
        events: Vec<String>,
    }

    impl RecordingSink {
        fn new(limit: usize) -> Self {
            Self {
                limit,
                events: Vec::new(),
            }
        }
    }

    impl ViewSink for RecordingSink {
        type Node = ();
        type View = Vec<String>;

        fn retention_cap(&self) -> usize {
            self.limit
        }

        fn root(&self) {}

        fn open(&mut self, _node: (), kind: ValueKind) -> Result<usize, ScanError> {
            self.events.push(format!("open {kind:?}"));
            Ok(self.limit)
        }

        fn member(&mut self, _object: (), key: Key<'_>) -> Result<Option<()>, ScanError> {
            self.events.push(format!("member {key:?}"));
            Ok(Some(()))
        }

        fn element(&mut self, _array: (), index: u64) -> Result<Option<()>, ScanError> {
            self.events.push(format!("element {index}"));
            Ok(Some(()))
        }

        fn scalar(&mut self, _node: (), value: Scalar<'_>) -> Result<(), ScanError> {
            self.events.push(format!("scalar {value:?}"));
            Ok(())
        }

        fn close(&mut self, _node: ()) -> Result<(), ScanError> {
            self.events.push("close".to_owned());
            Ok(())
        }

        fn finish(self) -> Result<Vec<String>, ScanError> {
            Ok(self.events)
        }
    }

    fn scan_chunks<'a>(
        chunks: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<Vec<String>, ScanError> {
        let mut scanner = JsonScanner::new(RecordingSink::new(64));
        for chunk in chunks {
            scanner.feed(chunk)?;
        }
        scanner.finish()
    }

    fn scan_whole(document: &[u8]) -> Result<Vec<String>, ScanError> {
        scan_chunks([document])
    }

    /// Small xorshift generator: deterministic chunking without a dependency.
    struct XorShift(u64);

    impl XorShift {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
    }

    /// Every two-way split, every byte-by-byte feed, and for short documents
    /// every three-way split, must agree with the single feed.
    fn assert_split_invariant(document: &[u8]) {
        let whole = scan_whole(document);
        for split in 0..=document.len() {
            let (head, tail) = document.split_at(split);
            assert_eq!(
                scan_chunks([head, tail]),
                whole,
                "split at {split} of {:?}",
                String::from_utf8_lossy(document)
            );
        }
        assert_eq!(scan_chunks(document.chunks(1)), whole);
        if document.len() <= 48 {
            for first in 0..=document.len() {
                for second in first..=document.len() {
                    let chunks = [
                        &document[..first],
                        &document[first..second],
                        &document[second..],
                    ];
                    assert_eq!(scan_chunks(chunks), whole);
                }
            }
        }
    }

    const VALID: &[&str] = &[
        r#"{}"#,
        r#"[]"#,
        r#"  { "a" : [ 1 , -0 , 0.5 , -1.25e+10 , 3E-2 , 12345678901234567890 ] } "#,
        r#"{"s":"plain","e":"q\"b\\s\/b\bf\fn\nr\rt\t","u":"\u00e9\u4e2d\ud83d\ude00"}"#,
        "{\"raw\":\"caf\u{e9} \u{4e2d} \u{1f600}\",\"k\u{e9}y\":true}",
        r#"{"t":true,"f":false,"n":null,"nested":{"x":[{"y":[[]]},{}]}}"#,
        r#""top-level string""#,
        "-12.5e3",
        "0",
        "null",
        r#"{"a":"\u0000\u001f"}"#,
    ];

    #[test]
    fn valid_documents_agree_across_every_split() {
        for document in VALID {
            assert!(scan_whole(document.as_bytes()).is_ok(), "{document}");
            assert_eq!(validate_document(document.as_bytes()), Ok(()), "{document}");
            assert_split_invariant(document.as_bytes());
        }
    }

    #[test]
    fn invalid_documents_fail_identically_across_every_split() {
        let invalid: &[&[u8]] = &[
            br#"{"a":1,}"#,
            br#"[1,]"#,
            br#"{"a" 1}"#,
            br#"{1:2}"#,
            br#"{"a":01}"#,
            br#"{"a":-}"#,
            br#"{"a":1.}"#,
            br#"{"a":1e}"#,
            br#"{"a":+1}"#,
            br#"{"a":.5}"#,
            br#"{"a":tru}"#,
            br#"{"a":nul}"#,
            br#"{"a":truex}"#,
            br#"{"a":"\x"}"#,
            br#"{"a":"\u12G4"}"#,
            br#"{"a":"\ud800"}"#,
            br#"{"a":"\ud800\u0041"}"#,
            br#"{"a":"\ud800x"}"#,
            br#"{"a":"\udc00"}"#,
            b"{\"a\":\"line\nbreak\"}",
            b"{\"a\":\"\xc0\x80\"}",
            b"{\"a\":\"\xed\xa0\x80\"}",
            b"{\"a\":\"\xf5\x80\x80\x80\"}",
            b"{\"a\":\"\xe4\xb8\"}",
            b"{\"a\":\"\xf4\x90\x80\x80\"}",
            b"\xef\xbb\xbf{}",
            br#"{} {}"#,
            br#"{}x"#,
            br#"<html>gateway</html>"#,
            br#"1 2"#,
        ];
        for document in invalid {
            let whole = scan_whole(document);
            assert!(
                whole.is_err() && whole != Err(ScanError::Truncated),
                "{:?} -> {whole:?}",
                String::from_utf8_lossy(document)
            );
            assert_eq!(validate_document(document), whole.map(|_| ()));
            assert_split_invariant(document);
        }
    }

    #[test]
    fn validation_depth_counts_containers_and_not_escaped_string_content() {
        let quoted = r#"\"[\u005b{]"#.repeat(MAX_DEPTH + 1);
        let document = format!(r#"{{"text":"{quoted}","nested":[{{"v":1}}]}}"#);
        assert_eq!(validate_document(document.as_bytes()), Ok(()));
        let nested = |depth| format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
        assert_eq!(validate_document(nested(MAX_DEPTH).as_bytes()), Ok(()));
        assert_eq!(
            validate_document(nested(MAX_DEPTH + 1).as_bytes()),
            Err(ScanError::NestingTooDeep {
                offset: MAX_DEPTH as u64,
                limit: MAX_DEPTH,
            }),
        );
    }

    #[test]
    fn validation_only_entry_requires_one_complete_grammatical_document() {
        assert_eq!(
            validate_document(b"{}[]"),
            Err(ScanError::TrailingContent { offset: 2 })
        );
        assert_eq!(
            validate_document(br#"[1,]"#),
            Err(ScanError::Syntax { offset: 3 })
        );
        assert_eq!(
            validate_document(br#"{"unfinished":["#),
            Err(ScanError::Truncated)
        );
    }

    #[test]
    fn specific_errors_are_reported() {
        assert_eq!(
            scan_whole(br#"{"a":"\q"}"#),
            Err(ScanError::InvalidEscape { offset: 7 })
        );
        assert_eq!(
            scan_whole(br#"{"a":"\udc00"}"#),
            Err(ScanError::InvalidEscape { offset: 11 })
        );
        assert_eq!(
            scan_whole(b"{\"a\":\"\xc0\"}"),
            Err(ScanError::InvalidUtf8 { offset: 6 })
        );
        assert_eq!(
            scan_whole(b"{\"a\":\"\x01\"}"),
            Err(ScanError::ControlCharacter { offset: 6 })
        );
        assert_eq!(
            scan_whole(br#"{} []"#),
            Err(ScanError::TrailingContent { offset: 3 })
        );
        assert_eq!(
            scan_whole(br#"{"a":1,}"#),
            Err(ScanError::Syntax { offset: 7 })
        );
        assert_eq!(scan_whole(b""), Err(ScanError::Truncated));
        assert_eq!(scan_whole(b"   "), Err(ScanError::Truncated));
    }

    #[test]
    fn every_proper_prefix_is_truncated() {
        for document in VALID {
            let bytes = document.as_bytes();
            // A top-level number is complete at any digit boundary.
            if bytes[0] == b'-' || bytes[0].is_ascii_digit() {
                continue;
            }
            let end = bytes
                .iter()
                .rposition(|b| !b.is_ascii_whitespace())
                .unwrap();
            for cut in 0..=end {
                assert_eq!(
                    scan_whole(&bytes[..cut]),
                    Err(ScanError::Truncated),
                    "{document} cut at {cut}"
                );
            }
        }
    }

    #[test]
    fn decodes_escapes_and_multibyte_text() {
        let events = scan_whole(
            br#"{"s":"q\"b\\s\/b\bf\fn\nr\rt\t\u00e9\u4E2D\ud83d\ude00", "k\u00e9y": 1}"#,
        )
        .unwrap();
        assert!(events.contains(&format!(
            "scalar {:?}",
            Scalar::Str("q\"b\\s/b\u{8}f\u{c}n\nr\rt\t\u{e9}\u{4e2d}\u{1f600}")
        )));
        assert!(events.contains(&format!("member {:?}", Key::Text("k\u{e9}y"))));
    }

    #[test]
    fn numbers_report_integer_shape() {
        let mut scanner = JsonScanner::new(RecordingSink::new(64));
        scanner
            .feed(br#"[7, -0, 1.0, 2e3, 9223372036854775808]"#)
            .unwrap();
        let events = scanner.finish().unwrap();
        let numbers: Vec<_> = events
            .iter()
            .filter(|event| event.starts_with("scalar Number"))
            .collect();
        assert_eq!(numbers.len(), 5);
        let parse = |text: &str, integer| NumberText { text, integer }.as_i64();
        assert_eq!(parse("7", true), Some(7));
        assert_eq!(parse("-0", true), Some(0));
        assert_eq!(parse("1.0", false), None);
        assert_eq!(parse("9223372036854775808", true), None);
        assert!(numbers[2].contains("integer: false"));
        assert!(numbers[0].contains("integer: true"));
    }

    #[test]
    fn nesting_limit_is_enforced() {
        let nested = |depth: usize| {
            let mut document = "[".repeat(depth);
            document.push_str(&"]".repeat(depth));
            document
        };
        assert!(scan_whole(nested(MAX_DEPTH).as_bytes()).is_ok());
        assert_eq!(
            scan_whole(nested(MAX_DEPTH + 1).as_bytes()),
            Err(ScanError::NestingTooDeep {
                offset: MAX_DEPTH as u64,
                limit: MAX_DEPTH,
            })
        );
        // Skipped subtrees count too.
        let mut scanner = JsonScanner::new(SkippingSink);
        let mut document = String::from("{\"skip\":");
        document.push_str(&nested(MAX_DEPTH));
        document.push('}');
        assert!(matches!(
            scanner.feed(document.as_bytes()),
            Err(ScanError::NestingTooDeep { .. })
        ));
    }

    #[test]
    fn overlong_values_and_keys_are_reported_not_retained() {
        let long = "x".repeat(65);
        let document = format!(r#"{{"{long}":"{long}","n":{}}}"#, "1".repeat(65));
        let events = scan_whole(document.as_bytes()).unwrap();
        assert!(!events.contains(&format!("member {:?}", Key::Overlong)));
        // Keys are retained up to MAX_KEY_LEN, values up to the sink limit.
        assert!(events.contains(&format!("member {:?}", Key::Text(&long))));
        assert!(events.contains(&format!("scalar {:?}", Scalar::Overlong(ValueKind::String))));
        assert!(events.contains(&format!("scalar {:?}", Scalar::Overlong(ValueKind::Number))));
        let long_key = "k".repeat(MAX_KEY_LEN + 1);
        let events = scan_whole(format!(r#"{{"{long_key}":1}}"#).as_bytes()).unwrap();
        assert!(events.contains(&format!("member {:?}", Key::Overlong)));
    }

    #[test]
    fn errors_are_sticky() {
        let mut scanner = JsonScanner::new(RecordingSink::new(64));
        let error = scanner.feed(b"{]").unwrap_err();
        assert_eq!(scanner.feed(b"}"), Err(error));
        assert_eq!(scanner.finish(), Err(error));
    }

    fn big_document() -> Vec<u8> {
        let mut document = String::from("{\"items\":[");
        for index in 0..2_000 {
            if index > 0 {
                document.push(',');
            }
            document.push_str(&format!(
                "{{\"id\":{index},\"neg\":-{index}.{index}e-3,\"name\":\"n\\u00e9\u{4e2d}\\\"{index}\\ud83d\\ude00\",\
                 \"tags\":[true,false,null,\"\u{1f600}\"],\"o\":{{\"k\":\"v\\n\"}}}}"
            ));
        }
        document.push_str("]}");
        document.into_bytes()
    }

    #[test]
    fn random_chunking_of_a_large_document_agrees() {
        let document = big_document();
        let whole = scan_whole(&document).unwrap();
        let mut rng = XorShift(0x9e37_79b9_7f4a_7c15);
        for round in 0..32 {
            let mut chunks = Vec::new();
            let mut rest = document.as_slice();
            let max = if round % 2 == 0 { 7 } else { 4096 };
            while !rest.is_empty() {
                let size = (rng.next() as usize % max + 1).min(rest.len());
                let (head, tail) = rest.split_at(size);
                chunks.push(head);
                rest = tail;
            }
            assert_eq!(scan_chunks(chunks).as_ref(), Ok(&whole), "round {round}");
        }
    }

    const CHUNK: usize = 64 * 1024;
    const HUGE: usize = 256 * 1024 * 1024;

    /// Feed `{"<key>":"<HUGE bytes of filler>"}` in 64 KiB chunks, generated
    /// on the fly, asserting the scanner's own retention after every chunk.
    fn feed_huge_string<S: ViewSink>(scanner: &mut JsonScanner<S>, key: &str, filler: &[u8]) {
        let fixed = scanner.retained_bytes();
        scanner.feed(format!("{{\"{key}\":\"").as_bytes()).unwrap();
        let mut chunk = Vec::with_capacity(CHUNK);
        while chunk.len() < CHUNK {
            chunk.extend_from_slice(filler);
        }
        chunk.truncate(CHUNK - CHUNK % filler.len());
        let mut fed = 0;
        while fed < HUGE {
            scanner.feed(&chunk).unwrap();
            fed += chunk.len();
            assert_eq!(scanner.retained_bytes(), fixed);
        }
        scanner.feed(b"\"}").unwrap();
        assert_eq!(scanner.retained_bytes(), fixed);
    }

    #[test]
    fn skipping_a_256_mib_string_keeps_retention_fixed() {
        let mut scanner = JsonScanner::new(SkippingSink);
        let fixed = scanner.retained_bytes();
        assert!(fixed <= MAX_KEY_LEN + MAX_DEPTH * 64);
        feed_huge_string(&mut scanner, "skipped", b"abcdefgh");
        assert!(scanner.consumed() > HUGE as u64);
        scanner.finish().unwrap();
    }

    #[test]
    fn an_interested_256_mib_string_overflows_without_growing() {
        let mut scanner = JsonScanner::new(RecordingSink::new(256));
        let fixed = scanner.retained_bytes();
        assert!(fixed <= MAX_KEY_LEN + MAX_DEPTH * 64);
        // Escapes and multi-byte text take the slow path; keep it in the mix.
        feed_huge_string(
            &mut scanner,
            "kept",
            "ab\\n\u{e9}\\u4e2d\u{1f600}cd".as_bytes(),
        );
        let events = scanner.finish().unwrap();
        assert!(events.contains(&format!("scalar {:?}", Scalar::Overlong(ValueKind::String))));
    }
}
