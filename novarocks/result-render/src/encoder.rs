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

//! Immutable Arrow input and bounded ClientRows encoding state.
use std::fmt::Write;
#[cfg(test)]
thread_local! {static FLOAT_FORMATS:std::cell::Cell<usize>=const{std::cell::Cell::new(0)};}
use crate::{
    BoundedClientRowEncoder, RenderError, RenderErrorKind as E, RenderTurn, RenderTurnStatus,
};
use arrow::array::*;
use arrow::datatypes::{DataType, Int32Type, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, Timelike};
use novarocks_result_contract::{
    ClientRenderSchema, FrozenRootOutput, NativeRenderType as N, RenderField, RenderTimeUnit,
    RootOutputContract, RootProfileV1 as V,
};
use std::mem::size_of;
use std::ops::Range;
use std::sync::Arc;
#[path = "json_cursor.rs"]
mod json_cursor;
#[path = "variant_cursor.rs"]
mod variant_cursor;
use json_cursor::{JsonCursor, Utf8};
use variant_cursor::{Collection, VariantView};

fn error(kind: E) -> RenderError {
    RenderError {
        kind,
        output_ordinal: None,
        required: None,
    }
}
fn invalid() -> RenderError {
    error(E::CursorInvariant)
}
fn invalid_value() -> RenderError {
    error(E::InvalidValue)
}
fn extent_error(kind: E, required: usize) -> RenderError {
    RenderError {
        kind,
        output_ordinal: None,
        required: u64::try_from(required).ok(),
    }
}
fn down<T: 'static>(a: &ArrayRef) -> Result<&T, RenderError> {
    a.as_any()
        .downcast_ref()
        .ok_or_else(|| error(E::UnsupportedCarrier))
}
fn bytes(a: &ArrayRef, row: usize) -> Result<&[u8], RenderError> {
    match a.data_type() {
        DataType::Utf8 => Ok(down::<StringArray>(a)?.value(row).as_bytes()),
        DataType::LargeUtf8 => Ok(down::<LargeStringArray>(a)?.value(row).as_bytes()),
        DataType::Binary => Ok(down::<BinaryArray>(a)?.value(row)),
        DataType::LargeBinary => Ok(down::<LargeBinaryArray>(a)?.value(row)),
        DataType::FixedSizeBinary(_) => Ok(down::<FixedSizeBinaryArray>(a)?.value(row)),
        _ => Err(error(E::UnsupportedCarrier)),
    }
}
#[derive(Clone, Copy)]
struct Path {
    column: u16,
    depth: u8,
    children: [u16; 64],
}
impl Path {
    fn root(column: usize) -> Self {
        Self {
            column: column as u16,
            depth: 0,
            children: [0; 64],
        }
    }
    fn child(mut self, index: usize) -> Result<Self, RenderError> {
        if self.depth as usize >= 64 {
            return Err(extent_error(E::DepthLimit, usize::from(self.depth) + 1));
        }
        if index > u16::MAX as usize {
            return Err(error(E::SchemaMismatch));
        }
        self.children[self.depth as usize] = index as u16;
        self.depth += 1;
        Ok(self)
    }
    fn field(self, s: &ClientRenderSchema) -> Result<&RenderField, RenderError> {
        let mut f = &s.columns()[self.column as usize].field;
        for i in 0..self.depth as usize {
            let n = self.children[i] as usize;
            f = match &f.native_type {
                N::List(child) if n == 0 => child,
                N::Map { key, .. } if n == 0 => key,
                N::Map { value, .. } if n == 1 => value,
                N::Struct(fields) => &fields.get(n).ok_or_else(invalid)?.field,
                _ => return Err(error(E::SchemaMismatch)),
            };
        }
        Ok(f)
    }
}
#[derive(Clone)]
enum Source {
    Array {
        array: ArrayRef,
        row: usize,
        range: Option<Range<usize>>,
    },
}
impl Source {
    fn metadata_work(&self) -> usize {
        match self {
            Self::Array { .. } => 16,
        }
    }
    fn array(array: ArrayRef, row: usize) -> Self {
        Self::Array {
            array,
            row,
            range: None,
        }
    }
    fn data(&self) -> Result<&[u8], RenderError> {
        match self {
            Self::Array { array, row, range } => {
                let b = bytes(array, *row)?;
                if let Some(r) = range {
                    b.get(r.clone()).ok_or_else(invalid)
                } else {
                    Ok(b)
                }
            }
        }
    }
    fn slice(&self, range: Range<usize>) -> Result<Self, RenderError> {
        match self {
            Self::Array {
                array,
                row,
                range: None,
            } => Ok(Self::Array {
                array: array.clone(),
                row: *row,
                range: Some(range),
            }),
            _ => Err(invalid()),
        }
    }
}
#[derive(Clone, Copy)]
enum Mode {
    Json,
    Base64,
}
struct Stream {
    source: Source,
    at: usize,
    mode: Mode,
    pending: [u8; 8],
    pending_at: usize,
    pending_len: usize,
    json: Option<JsonCursor>,
    utf8: Option<Utf8>,
}
impl Stream {
    fn new(source: Source, mode: Mode) -> Self {
        Self {
            source,
            at: 0,
            mode,
            pending: [0; 8],
            pending_at: 0,
            pending_len: 0,
            json: None,
            utf8: None,
        }
    }
}
#[derive(Clone)]
struct Atom {
    data: [u8; 512],
    len: usize,
    at: usize,
}
impl Atom {
    fn new() -> Self {
        Self {
            data: [0; 512],
            len: 0,
            at: 0,
        }
    }
    fn literal(b: &[u8]) -> Self {
        let mut x = Self::new();
        x.data[..b.len()].copy_from_slice(b);
        x.len = b.len();
        x
    }
}
impl std::fmt::Write for Atom {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let end = self.len.checked_add(s.len()).ok_or(std::fmt::Error)?;
        if end > self.data.len() {
            return Err(std::fmt::Error);
        }
        self.data[self.len..end].copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}
macro_rules! atom {($($arg:tt)*)=>{{let mut a=Atom::new();write!(&mut a,$($arg)*).map_err(|_|error(E::CursorInvariant))?;a}}}
#[derive(Clone)]
struct Variant {
    source: Source,
    view: VariantView,
    range: Range<usize>,
    depth: usize,
}
#[expect(
    clippy::large_enum_variant,
    reason = "Inline formatting atoms share the fixed, accounted cursor stack and must not allocate per cell"
)]
enum EncodeStep {
    JsonEscape(bool),
    TrinoKey {
        array: ArrayRef,
        row: usize,
        path: Path,
    },
    Value {
        array: ArrayRef,
        row: usize,
        path: Path,
    },
    List {
        array: ArrayRef,
        next: usize,
        end: usize,
        path: Path,
        first: bool,
    },
    Map {
        array: ArrayRef,
        next: usize,
        end: usize,
        path: Path,
        phase: u8,
        first: bool,
    },
    Struct {
        array: ArrayRef,
        row: usize,
        path: Path,
        next: usize,
    },
    Stream(Stream),
    Atom(Atom),
    Variant(Variant),
    VariantCollection {
        value: Variant,
        info: Collection,
        next: usize,
        phase: u8,
        object: bool,
    },
    Metadata {
        source: Source,
        view: VariantView,
        next: usize,
        at: usize,
        utf8: Utf8,
    },
}
fn stream_next(
    stream: &mut Stream,
    data: &[u8],
    b: &mut Budget,
    elements: &mut usize,
) -> Result<Poll, RenderError> {
    loop {
        if stream.pending_at < stream.pending_len {
            let byte = stream.pending[stream.pending_at];
            stream.pending_at += 1;
            b.scan(1);
            return Ok(Poll::Byte(byte));
        }
        if stream.json.is_some() && b.cells == V::CELLS_PER_TURN {
            return Ok(Poll::Yield);
        }

        if stream.at == data.len() {
            if let Some(j) = &mut stream.json {
                j.finish()?;
            }
            if let Some(u) = stream.utf8 {
                u.finish()?;
            }
            return Ok(Poll::Done);
        }
        stream.pending_at = 0;
        stream.pending_len = 0;
        let c = data[stream.at];
        match stream.mode {
            Mode::Base64 => {
                const ABC: &[u8; 64] =
                    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
                let n = (data.len() - stream.at).min(3);
                let a = data[stream.at];
                let c1 = if n > 1 { data[stream.at + 1] } else { 0 };
                let c2 = if n > 2 { data[stream.at + 2] } else { 0 };
                stream.pending[..4].copy_from_slice(&[
                    ABC[(a >> 2) as usize],
                    ABC[(((a & 3) << 4) | (c1 >> 4)) as usize],
                    if n > 1 {
                        ABC[(((c1 & 15) << 2) | (c2 >> 6)) as usize]
                    } else {
                        b'='
                    },
                    if n > 2 { ABC[(c2 & 63) as usize] } else { b'=' },
                ]);
                stream.pending_len = 4;
                stream.at += n;
                b.scan(n);
            }
            Mode::Json => {
                b.scan(1);
                stream.at += 1;
                if let Some(j) = &mut stream.json
                    && j.feed(c)?
                {
                    // Cell capacity was reserved before feed. Never rewind a
                    // byte after the parser has accepted its state transition.
                    if !b.cell() {
                        return Err(error(E::CursorInvariant));
                    }
                    *elements = elements
                        .checked_add(1)
                        .ok_or_else(|| error(E::ArithmeticOverflow))?;
                    if *elements > V::MAX_ELEMENTS_PER_ROW {
                        return Err(extent_error(E::ElementLimit, *elements));
                    }
                }
                if let Some(u) = &mut stream.utf8 {
                    u.feed(c)?;
                }
                let mut escaped = [0; 6];
                let count = trino_json::escape_byte(c, &mut escaped);
                stream.pending[..count].copy_from_slice(&escaped[..count]);
                stream.pending_len = count;
            }
        }
    }
}
struct Budget {
    work: usize,
    examined: usize,
    emitted: usize,
    cells: usize,
}

/// Render a session SQL double with the same Java-compatible decimal author
/// used for Trino result values. Session effects carry text, without JSON quotes.
pub fn render_session_double(value: f64) -> Result<String, RenderError> {
    let atom = trino_json::float64(value, &mut Budget::new())?;
    let bytes = &atom.data[..atom.len];
    let bytes = if value.is_finite() {
        bytes
    } else {
        &bytes[1..bytes.len() - 1]
    };
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| RenderError {
            kind: crate::RenderErrorKind::CursorInvariant,
            output_ordinal: None,
            required: None,
        })
}
impl Budget {
    fn new() -> Self {
        Self {
            work: 0,
            examined: 0,
            emitted: 0,
            cells: 0,
        }
    }
    fn room(&self, n: usize) -> bool {
        self.work + n <= V::EMIT_BYTES_PER_TURN
    }
    fn scan(&mut self, n: usize) {
        self.work += n;
        self.examined += n;
    }
    fn emit(&mut self, n: usize) {
        self.work += n;
        self.emitted += n;
    }
    fn cell(&mut self) -> bool {
        if self.cells == V::CELLS_PER_TURN {
            return false;
        }
        self.cells += 1;
        true
    }
}
enum Poll {
    Byte(u8),
    Done,
    Yield,
}
const CELL_TASK_CAPACITY: usize = 196;

struct CellCursor {
    json_escape: bool,
    escaped: [u8; 6],
    escaped_at: usize,
    escaped_len: usize,
    stack: Vec<EncodeStep>,
    elements: usize,
}
impl CellCursor {
    fn new() -> Self {
        Self {
            json_escape: false,
            escaped: [0; 6],
            escaped_at: 0,
            escaped_len: 0,
            stack: Vec::with_capacity(CELL_TASK_CAPACITY),
            elements: 0,
        }
    }
    fn reset(&mut self, array: ArrayRef, row: usize, path: Path, base: usize) {
        self.stack.clear();
        self.json_escape = false;
        self.escaped_at = 0;
        self.escaped_len = 0;
        self.elements = base;
        self.stack.push(EncodeStep::Value { array, row, path });
    }
    fn push(&mut self, t: EncodeStep) -> Result<(), RenderError> {
        if self.stack.len() >= CELL_TASK_CAPACITY {
            return Err(extent_error(E::WorkStackLimit, self.stack.len() + 1));
        }
        self.stack.push(t);
        Ok(())
    }
    fn literal(&mut self, b: &[u8]) -> Result<(), RenderError> {
        self.push(EncodeStep::Atom(Atom::literal(b)))
    }
    fn element(&mut self) -> Result<(), RenderError> {
        self.elements = self
            .elements
            .checked_add(1)
            .ok_or_else(|| error(E::ArithmeticOverflow))?;
        if self.elements > V::MAX_ELEMENTS_PER_ROW {
            return Err(extent_error(E::ElementLimit, self.elements));
        }
        Ok(())
    }
    fn quoted(&mut self, source: Source, mode: Mode, quote: u8) -> Result<(), RenderError> {
        self.literal(&[quote])?;
        let mut stream = Stream::new(source, mode);
        if matches!(mode, Mode::Json) {
            stream.utf8 = Some(Utf8::default());
        }
        self.push(EncodeStep::Stream(stream))?;
        self.literal(&[quote])
    }
    fn drain(
        &mut self,
        b: &mut Budget,
        mut output: Option<&mut [u8]>,
        limit: usize,
        external: bool,
    ) -> Result<usize, RenderError> {
        if self.json_escape || self.escaped_at < self.escaped_len {
            return Ok(0);
        }
        let limit = limit.min((V::EMIT_BYTES_PER_TURN - b.work) / 2);
        if limit == 0 {
            return Ok(0);
        }
        let charge = |b: &mut Budget, n: usize| {
            if external {
                b.emit(n);
            } else {
                b.scan(n);
            }
        };
        match self.stack.last_mut() {
            Some(EncodeStep::Atom(a)) => {
                let n = (a.len - a.at).min(limit);
                if let Some(out) = output {
                    out[..n].copy_from_slice(&a.data[a.at..a.at + n]);
                }
                a.at += n;
                b.scan(n);
                charge(b, n);
                Ok(n)
            }
            Some(EncodeStep::Stream(stream)) => {
                let source = stream.source.clone();
                let metadata = source.metadata_work();
                if !b.room(metadata + 2) {
                    return Ok(0);
                }
                b.scan(metadata);
                let data = source.data()?;
                let mut n = 0;
                while n < limit && b.room(4096) {
                    match stream_next(stream, data, b, &mut self.elements)? {
                        Poll::Byte(byte) => {
                            if let Some(out) = output.as_deref_mut() {
                                out[n] = byte;
                            }
                            n += 1;
                            charge(b, 1);
                        }
                        Poll::Done | Poll::Yield => break,
                    }
                }
                Ok(n)
            }
            _ => Ok(0),
        }
    }
    fn next(&mut self, s: &ClientRenderSchema, b: &mut Budget) -> Result<Poll, RenderError> {
        if !b.room(4096) {
            return Ok(Poll::Yield);
        }
        if self.escaped_at < self.escaped_len {
            let byte = self.escaped[self.escaped_at];
            self.escaped_at += 1;
            b.scan(1);
            return Ok(Poll::Byte(byte));
        }
        let poll = self.next_raw(s, b)?;
        if let Poll::Byte(byte) = poll
            && self.json_escape
        {
            self.escaped_len = trino_json::escape_byte(byte, &mut self.escaped);
            self.escaped_at = 1;
            b.scan(1);
            return Ok(Poll::Byte(self.escaped[0]));
        }
        Ok(poll)
    }
    fn next_raw(&mut self, s: &ClientRenderSchema, b: &mut Budget) -> Result<Poll, RenderError> {
        loop {
            // Leave capacity for both a maximum fixed descriptor/atom operation
            // and the caller's destination byte. No uncharged scans occur here.
            if !b.room(4096) {
                return Ok(Poll::Yield);
            }
            if let Some(EncodeStep::Atom(a)) = self.stack.last_mut() {
                if a.at < a.len {
                    let byte = a.data[a.at];
                    a.at += 1;
                    b.scan(1);
                    return Ok(Poll::Byte(byte));
                }
                self.stack.pop();
                continue;
            }
            if let Some(EncodeStep::Stream(stream)) = self.stack.last_mut() {
                let source = stream.source.clone();
                b.scan(source.metadata_work());
                match stream_next(stream, source.data()?, b, &mut self.elements)? {
                    Poll::Done => {
                        self.stack.pop();
                        continue;
                    }
                    poll => return Ok(poll),
                }
            }
            if let Some(EncodeStep::Metadata {
                source,
                view,
                next,
                at,
                utf8,
            }) = self.stack.last_mut()
            {
                if *next == view.dictionary_size {
                    self.stack.pop();
                    continue;
                }
                let data = source.data()?;
                let range = view.key(data, *next)?;
                b.scan(32);
                let n = (range.len() - *at).min(V::EMIT_BYTES_PER_TURN - b.work - 1);
                for c in &data[range.start + *at..range.start + *at + n] {
                    utf8.feed(*c)?;
                }
                *at += n;
                b.scan(n);
                if *at == range.len() {
                    utf8.finish()?;
                    if !b.cell() {
                        return Ok(Poll::Yield);
                    }
                    *next += 1;
                    *at = 0;
                    *utf8 = Utf8::default();
                }
                continue;
            }
            let Some(task) = self.stack.pop() else {
                return Ok(Poll::Done);
            };
            match task {
                EncodeStep::JsonEscape(enabled) => self.json_escape = enabled,
                EncodeStep::TrinoKey { array, row, path } => {
                    if !b.cell() {
                        self.push(EncodeStep::TrinoKey { array, row, path })?;
                        return Ok(Poll::Yield);
                    }
                    self.element()?;
                    b.scan(32 + usize::from(path.depth) * 2);
                    self.trino_value(array, row, path, s, true, b)?;
                }
                EncodeStep::Stream(_) | EncodeStep::Atom(_) => {
                    unreachable!("byte tasks are advanced in place")
                }
                EncodeStep::Value { array, row, path } => {
                    if !b.cell() {
                        self.push(EncodeStep::Value { array, row, path })?;
                        return Ok(Poll::Yield);
                    }
                    self.element()?;
                    b.scan(32 + usize::from(path.depth) * 2);
                    if row >= array.len() {
                        return Err(error(E::SchemaMismatch));
                    }
                    let field = path.field(s)?;
                    let (array, row) = resolve_dictionary(array, row)?;
                    if array.is_null(row) {
                        if !field.nullable && !matches!(field.native_type, N::Null) {
                            return Err(error(E::SchemaMismatch));
                        }
                        self.literal(b"null")?;
                        continue;
                    }
                    self.trino_value(array, row, path, s, false, b)?;
                }
                EncodeStep::List {
                    array,
                    next,
                    end,
                    path,
                    first,
                } => {
                    if next < end {
                        let child = path.child(0)?;
                        self.push(EncodeStep::List {
                            array: array.clone(),
                            next: next + 1,
                            end,
                            path,
                            first: false,
                        })?;
                        self.push(EncodeStep::Value {
                            array,
                            row: next,
                            path: child,
                        })?;
                        if !first {
                            self.literal(b",")?;
                        }
                    }
                }
                EncodeStep::Map {
                    array,
                    next,
                    end,
                    path,
                    phase,
                    first,
                } => {
                    if next < end {
                        let m = down::<MapArray>(&array)?;
                        let a = if phase == 0 {
                            m.keys().clone()
                        } else {
                            m.values().clone()
                        };
                        self.push(EncodeStep::Map {
                            array,
                            next: if phase == 0 { next } else { next + 1 },
                            end,
                            path,
                            phase: 1 - phase,
                            first: false,
                        })?;
                        let child = path.child(phase as usize)?;
                        self.push(if phase == 0 {
                            EncodeStep::TrinoKey {
                                array: a,
                                row: next,
                                path: child,
                            }
                        } else {
                            EncodeStep::Value {
                                array: a,
                                row: next,
                                path: child,
                            }
                        })?;
                        if phase == 1 {
                            self.literal(b":")?;
                        } else if !first {
                            self.literal(b",")?;
                        }
                    }
                }
                EncodeStep::Struct {
                    array,
                    row,
                    path,
                    next,
                } => {
                    let fields = if let N::Struct(fields) = &path.field(s)?.native_type {
                        fields
                    } else {
                        return Err(invalid());
                    };
                    if next < fields.len() {
                        let child = down::<StructArray>(&array)?.column(next).clone();
                        self.push(EncodeStep::Struct {
                            array,
                            row,
                            path,
                            next: next + 1,
                        })?;
                        self.push(EncodeStep::Value {
                            array: child,
                            row,
                            path: path.child(next)?,
                        })?;
                        if next != 0 {
                            self.literal(b",")?;
                        }
                    }
                }
                EncodeStep::Metadata { .. } => unreachable!("metadata is checked in place"),
                EncodeStep::Variant(value) => {
                    if !b.cell() {
                        self.push(EncodeStep::Variant(value))?;
                        return Ok(Poll::Yield);
                    }
                    self.element()?;
                    b.scan(32);
                    if value.depth > 64 {
                        return Err(error(E::DepthLimit));
                    }
                    self.variant(value, b)?;
                }
                EncodeStep::VariantCollection {
                    value,
                    info,
                    next,
                    phase,
                    object,
                } => {
                    if next < info.count {
                        let data = value.source.data()?;
                        let range = info.child(data, next, value.range.end)?;
                        if object && phase == 0 {
                            let id = variant_cursor::read(
                                data,
                                info.ids + next * info.id_width,
                                info.id_width,
                            )? as usize;
                            let key = value.view.key(data, id)?;
                            let source = value.source.slice(key)?;
                            self.push(EncodeStep::VariantCollection {
                                value,
                                info,
                                next,
                                phase: 1,
                                object,
                            })?;
                            let mut stream = Stream::new(source, Mode::Json);
                            stream.utf8 = Some(Utf8::default());
                            self.literal(b"\"")?;
                            self.push(EncodeStep::Stream(stream))?;
                            self.literal(b"\"")?;
                            if next != 0 {
                                self.literal(b",")?;
                            }
                        } else {
                            let mut child = value.clone();
                            child.range = range;
                            child.depth += 1;
                            self.push(EncodeStep::VariantCollection {
                                value,
                                info,
                                next: next + 1,
                                phase: 0,
                                object,
                            })?;
                            self.push(EncodeStep::Variant(child))?;
                            if object {
                                self.literal(b":")?;
                            } else if next != 0 {
                                self.literal(b",")?;
                            }
                        }
                    }
                }
            }
        }
    }
    fn variant(&mut self, v: Variant, b: &mut Budget) -> Result<(), RenderError> {
        let data = v.source.data()?;
        let bytes = data
            .get(v.range.clone())
            .ok_or_else(|| error(E::InvalidVariant))?;
        let head = *bytes.first().ok_or_else(|| error(E::InvalidVariant))?;
        match head & 3 {
            2 | 3 => {
                if v.depth >= V::MAX_DEPTH {
                    return Err(error(E::DepthLimit));
                }
                let object = head & 3 == 2;
                let info = Collection::parse(data, v.range.clone(), object)?;
                if info.count > V::MAX_ELEMENTS_PER_ROW {
                    return Err(extent_error(E::ElementLimit, self.elements));
                }
                self.literal(if object { b"}" } else { b"]" })?;
                self.push(EncodeStep::VariantCollection {
                    value: v,
                    info,
                    next: 0,
                    phase: 0,
                    object,
                })?;
                self.literal(if object { b"{" } else { b"[" })?;
            }
            1 => {
                let len = (head >> 2) as usize;
                if len + 1 != bytes.len() {
                    return Err(error(E::InvalidVariant));
                }
                self.quoted(
                    v.source.slice(v.range.start + 1..v.range.end)?,
                    Mode::Json,
                    b'"',
                )?;
            }
            0 => {
                let tag = head >> 2;
                let payload = &bytes[1..];
                // Each primitive is bounded and validated before formatting.
                let a = match tag {
                    0 => {
                        if !payload.is_empty() {
                            return Err(error(E::InvalidVariant));
                        }
                        Some(Atom::literal(b"null"))
                    }
                    1 | 2 => {
                        if !payload.is_empty() {
                            return Err(error(E::InvalidVariant));
                        }
                        Some(Atom::literal(if tag == 1 { b"true" } else { b"false" }))
                    }
                    3 => Some(atom!(
                        "{}",
                        i8::from_le_bytes(
                            payload.try_into().map_err(|_| error(E::InvalidVariant))?
                        )
                    )),
                    4 => Some(atom!(
                        "{}",
                        i16::from_le_bytes(
                            payload.try_into().map_err(|_| error(E::InvalidVariant))?
                        )
                    )),
                    5 => Some(atom!(
                        "{}",
                        i32::from_le_bytes(
                            payload.try_into().map_err(|_| error(E::InvalidVariant))?
                        )
                    )),
                    6 => Some(atom!(
                        "{}",
                        i64::from_le_bytes(
                            payload.try_into().map_err(|_| error(E::InvalidVariant))?
                        )
                    )),
                    7 | 14 => Some(if tag == 7 {
                        trino_json::float64(
                            f64::from_le_bytes(
                                payload.try_into().map_err(|_| error(E::InvalidVariant))?,
                            ),
                            b,
                        )?
                    } else {
                        trino_json::float32(
                            f32::from_le_bytes(
                                payload.try_into().map_err(|_| error(E::InvalidVariant))?,
                            ),
                            b,
                        )?
                    }),
                    8..=10 => {
                        let width = match tag {
                            8 => 4,
                            9 => 8,
                            _ => 16,
                        };
                        if payload.len() != width + 1 || payload[0] > 38 {
                            return Err(error(E::InvalidVariant));
                        }
                        let mut raw = [0; 16];
                        let sign = payload[width] & 0x80 != 0;
                        raw[..width].copy_from_slice(&payload[1..]);
                        if sign {
                            raw[width..].fill(0xff);
                        }
                        let mut a =
                            decimal128(i128::from_le_bytes(raw), payload[0] as usize, None)?;
                        a = trino_json::variant_decimal(a, payload[0])?;
                        Some(a)
                    }
                    11 => {
                        let days = i32::from_le_bytes(
                            payload.try_into().map_err(|_| error(E::InvalidVariant))?,
                        );
                        Some(atom!("\"{}\"", date_atom(days)?))
                    }
                    12 | 13 | 17 | 18 | 19 => {
                        let raw = i64::from_le_bytes(
                            payload.try_into().map_err(|_| error(E::InvalidVariant))?,
                        );
                        let unit = if matches!(tag, 18 | 19) {
                            RenderTimeUnit::Nanosecond
                        } else {
                            RenderTimeUnit::Microsecond
                        };
                        let ty = if tag == 17 {
                            N::Time { unit }
                        } else {
                            N::Timestamp {
                                unit,
                                timezone: None,
                            }
                        };
                        let mut text = trino_json::temporal(raw, unit, &ty, true)?;
                        if matches!(tag, 12 | 18) {
                            text.write_str(" UTC").map_err(|_| invalid())?;
                        }
                        Some(atom!("\"{}\"", text))
                    }
                    15 | 16 => {
                        let len = variant_cursor::read(payload, 0, 4)? as usize;
                        if len.checked_add(4) != Some(payload.len()) {
                            return Err(error(E::InvalidVariant));
                        }
                        self.quoted(
                            v.source.slice(v.range.start + 5..v.range.end)?,
                            if tag == 15 { Mode::Base64 } else { Mode::Json },
                            b'"',
                        )?;
                        None
                    }
                    20 => {
                        if payload.len() != 16 {
                            return Err(error(E::InvalidVariant));
                        }
                        let mut a = Atom::literal(b"\"");
                        for (i, byte) in payload.iter().enumerate() {
                            if matches!(i, 4 | 6 | 8 | 10) {
                                a.write_str("-").map_err(|_| invalid())?;
                            }
                            write!(&mut a, "{byte:02x}").map_err(|_| invalid())?;
                        }
                        a.write_str("\"").map_err(|_| invalid())?;
                        Some(a)
                    }
                    _ => return Err(error(E::InvalidVariant)),
                };
                if let Some(a) = a {
                    b.scan(a.len * 2 + 512);
                    self.push(EncodeStep::Atom(a))?;
                }
            }
            _ => return Err(error(E::InvalidVariant)),
        }
        Ok(())
    }
}
impl std::fmt::Display for Atom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(std::str::from_utf8(&self.data[..self.len]).map_err(|_| std::fmt::Error)?)
    }
}
fn resolve_dictionary(array: ArrayRef, row: usize) -> Result<(ArrayRef, usize), RenderError> {
    if matches!(array.data_type(), DataType::Dictionary(_, _)) {
        let d = down::<DictionaryArray<Int32Type>>(&array)?;
        if d.is_null(row) {
            return Ok((array, row));
        }
        let key = usize::try_from(d.keys().value(row)).map_err(|_| invalid())?;
        if key >= d.values().len() {
            return Err(invalid());
        }
        Ok((d.values().clone(), key))
    } else {
        Ok((array, row))
    }
}
fn list_values(array: &ArrayRef, row: usize) -> Result<(ArrayRef, usize, usize), RenderError> {
    let (a, start, end) = match array.data_type() {
        DataType::List(_) => {
            let x = down::<ListArray>(array)?;
            let o = x.value_offsets();
            (x.values().clone(), i64::from(o[row]), i64::from(o[row + 1]))
        }
        DataType::LargeList(_) => {
            let x = down::<LargeListArray>(array)?;
            let o = x.value_offsets();
            (x.values().clone(), o[row], o[row + 1])
        }
        _ => return Err(error(E::UnsupportedCarrier)),
    };
    let start = usize::try_from(start).map_err(|_| invalid())?;
    let end = usize::try_from(end).map_err(|_| invalid())?;
    if start > end || end > a.len() {
        return Err(invalid());
    }
    Ok((a, start, end))
}
fn year_atom(year: i32) -> Result<Atom, RenderError> {
    Ok(if (0..=9999).contains(&year) {
        atom!("{year:04}")
    } else if year < 0 {
        atom!("-{:04}", year.unsigned_abs())
    } else {
        atom!("+{year}")
    })
}
fn date_atom(days: i32) -> Result<Atom, RenderError> {
    let ce = 719163i64
        .checked_add(i64::from(days))
        .ok_or_else(invalid_value)?;
    let date = chrono::NaiveDate::from_num_days_from_ce_opt(
        i32::try_from(ce).map_err(|_| invalid_value())?,
    )
    .ok_or_else(invalid_value)?;
    Ok(atom!(
        "{}-{:02}-{:02}",
        year_atom(date.year())?,
        date.month(),
        date.day()
    ))
}
fn decimal128(value: i128, scale: usize, precision: Option<u8>) -> Result<Atom, RenderError> {
    let negative = value < 0;
    let abs = value.unsigned_abs();
    let digits = atom!("{abs}");
    if precision.is_some_and(|p| digits.len > p as usize) {
        return Err(invalid_value());
    }
    decimal_digits(&digits.data[..digits.len], negative, scale)
}
fn decimal_digits(digits: &[u8], negative: bool, scale: usize) -> Result<Atom, RenderError> {
    let mut a = Atom::new();
    if negative {
        a.write_str("-").map_err(|_| invalid_value())?;
    }
    if scale == 0 {
        a.write_str(std::str::from_utf8(digits).map_err(|_| invalid_value())?)
            .map_err(|_| invalid_value())?;
    } else if digits.len() > scale {
        a.write_str(
            std::str::from_utf8(&digits[..digits.len() - scale]).map_err(|_| invalid_value())?,
        )
        .map_err(|_| invalid_value())?;
        a.write_str(".").map_err(|_| invalid_value())?;
        a.write_str(
            std::str::from_utf8(&digits[digits.len() - scale..]).map_err(|_| invalid_value())?,
        )
        .map_err(|_| invalid_value())?;
    } else {
        a.write_str("0.").map_err(|_| invalid_value())?;
        for _ in 0..scale - digits.len() {
            a.write_str("0").map_err(|_| invalid_value())?;
        }
        a.write_str(std::str::from_utf8(digits).map_err(|_| invalid_value())?)
            .map_err(|_| invalid_value())?;
    }
    Ok(a)
}
fn decimal256(raw: [u8; 32], scale: usize, precision: u8) -> Result<Atom, RenderError> {
    let digits = novarocks_functions::decimal256_digits::original_digits(raw);
    if digits.digits().len() > precision as usize {
        return Err(invalid_value());
    }
    decimal_digits(digits.digits(), digits.negative(), scale)
}
fn time_raw(array: &ArrayRef, row: usize) -> Result<i64, RenderError> {
    match array.data_type() {
        DataType::Time32(TimeUnit::Second) => {
            Ok(i64::from(down::<Time32SecondArray>(array)?.value(row)))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            Ok(i64::from(down::<Time32MillisecondArray>(array)?.value(row)))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            Ok(down::<Time64MicrosecondArray>(array)?.value(row))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Ok(down::<Time64NanosecondArray>(array)?.value(row))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            Ok(down::<TimestampSecondArray>(array)?.value(row))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Ok(down::<TimestampMillisecondArray>(array)?.value(row))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok(down::<TimestampMicrosecondArray>(array)?.value(row))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Ok(down::<TimestampNanosecondArray>(array)?.value(row))
        }
        _ => Err(error(E::UnsupportedCarrier)),
    }
}
fn scalar_atom(array: &ArrayRef, row: usize, field: &RenderField) -> Result<Atom, RenderError> {
    Ok(match field.native_type {
        N::SignedInteger(8) => atom!("{}", down::<Int8Array>(array)?.value(row)),
        N::SignedInteger(16) => atom!("{}", down::<Int16Array>(array)?.value(row)),
        N::SignedInteger(32) => atom!("{}", down::<Int32Array>(array)?.value(row)),
        N::SignedInteger(64) => atom!("{}", down::<Int64Array>(array)?.value(row)),
        _ => return Err(error(E::UnsupportedPresentation)),
    })
}

fn unit_matches(unit: RenderTimeUnit, u: &TimeUnit) -> bool {
    matches!(
        (unit, u),
        (RenderTimeUnit::Second, TimeUnit::Second)
            | (RenderTimeUnit::Millisecond, TimeUnit::Millisecond)
            | (RenderTimeUnit::Microsecond, TimeUnit::Microsecond)
            | (RenderTimeUnit::Nanosecond, TimeUnit::Nanosecond)
    )
}
fn validate_field(f: &RenderField, d: &DataType, nested: bool) -> Result<(), RenderError> {
    if let DataType::Dictionary(k, v) = d {
        if matches!(k.as_ref(), DataType::Int32)
            && matches!(v.as_ref(), DataType::Utf8 | DataType::LargeUtf8)
            && matches!(f.native_type, N::String | N::Json)
        {
            return Ok(());
        }
        return Err(error(E::UnsupportedCarrier));
    }
    let good = match (&f.native_type, d) {
        (N::Null, DataType::Null)
        | (N::Boolean, DataType::Boolean)
        | (N::SignedInteger(8), DataType::Int8)
        | (N::SignedInteger(16), DataType::Int16)
        | (N::SignedInteger(32), DataType::Int32)
        | (N::SignedInteger(64), DataType::Int64)
        | (N::UnsignedInteger(8), DataType::UInt8)
        | (N::UnsignedInteger(16), DataType::UInt16)
        | (N::UnsignedInteger(32), DataType::UInt32)
        | (N::UnsignedInteger(64), DataType::UInt64)
        | (N::LargeInt, DataType::FixedSizeBinary(16))
        | (N::Float32, DataType::Float32)
        | (N::Float64, DataType::Float64)
        | (N::String | N::Json, DataType::Utf8 | DataType::LargeUtf8)
        | (N::Binary, DataType::Binary | DataType::LargeBinary)
        | (N::Opaque(_), DataType::Binary)
        | (N::Variant, DataType::LargeBinary)
        | (N::Date, DataType::Date32) => true,
        (
            N::Decimal {
                bits: 128,
                precision,
                scale,
            },
            DataType::Decimal128(p, s),
        ) => precision == p && scale == s,
        (
            N::Decimal {
                bits: 256,
                precision,
                scale,
            },
            DataType::Decimal256(p, s),
        ) if nested => precision == p && scale == s,
        (N::Time { unit }, DataType::Time32(u) | DataType::Time64(u)) => unit_matches(*unit, u),
        (N::Timestamp { unit, timezone }, DataType::Timestamp(u, tz)) => {
            unit_matches(*unit, u) && timezone.as_deref() == tz.as_deref()
        }
        (N::List(child), DataType::List(field) | DataType::LargeList(field)) => {
            if child.nullable != field.is_nullable() {
                return Err(error(E::SchemaMismatch));
            }
            validate_field(child, field.data_type(), true)?;
            true
        }
        (N::Map { key, value }, DataType::Map(entries, _)) => {
            if let DataType::Struct(fields) = entries.data_type() {
                if fields.len() != 2 {
                    return Err(error(E::SchemaMismatch));
                }
                for (r, c) in [(key, &fields[0]), (value, &fields[1])] {
                    if r.nullable != c.is_nullable() {
                        return Err(error(E::SchemaMismatch));
                    }
                    validate_field(r, c.data_type(), true)?;
                }
                true
            } else {
                false
            }
        }
        (N::Struct(fields), DataType::Struct(carrier)) => {
            if fields.len() != carrier.len() {
                return Err(error(E::SchemaMismatch));
            }
            for (r, c) in fields.iter().zip(carrier) {
                if r.name != *c.name() || r.field.nullable != c.is_nullable() {
                    return Err(error(E::SchemaMismatch));
                }
                validate_field(&r.field, c.data_type(), true)?;
            }
            true
        }
        _ => false,
    };
    if !good {
        return Err(error(
            if matches!(f.native_type, N::Decimal { bits: 256, .. }) && !nested {
                E::UnsupportedPresentation
            } else {
                E::UnsupportedCarrier
            },
        ));
    }
    Ok(())
}
#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Small,
    Count,
    Emit,
    ReadySmall,
    Complete,
    Cancelled,
    Failed,
}
/// The host pre-admits complete schema/input/scratch overlap and retains its
/// authority through the encoder's actual destruction. This cursor owns no
/// transport and emits into caller-owned unpublished segments.
enum SchemaOwner {
    Standalone(Arc<ClientRenderSchema>),
    Root(Arc<RootOutputContract>),
    TypedRoot(ValidatedClientInput),
}
impl std::ops::Deref for SchemaOwner {
    type Target = ClientRenderSchema;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Standalone(schema) => schema,
            Self::Root(contract) => {
                let FrozenRootOutput::ClientRows(schema) = contract.output() else {
                    unreachable!("root schema owner is validated before construction");
                };
                schema
            }
            Self::TypedRoot(input) => {
                let FrozenRootOutput::ClientRows(schema) = input.contract().output() else {
                    unreachable!("typed root proof validates its purpose");
                };
                schema
            }
        }
    }
}
struct ArrowClientRowEncoder {
    batch: RecordBatch,
    stage: Box<[u8; V::SMALL_ROW_BYTES]>,
    lengths: Box<[u32; V::MAX_COLUMNS]>,
    cursor: CellCursor,
    phase: Phase,
    row: usize,
    column: usize,
    cell_started: bool,
    cell_length: usize,
    row_length: usize,
    elements: usize,
    stage_len: usize,
    stage_at: usize,
    prefix_at: usize,
    cell_atom: Atom,
    // Every batch/cursor array owner is destroyed before its type creator
    // responsibility. Independent cursors retain this immutable owner.
    schema: SchemaOwner,
}

/// Complete input semantics and this renderer's installed layout capability
/// are checked once. The retained input owner proves each batch schema before
/// a cursor can reuse the result, independently of per-row encoding checks.
#[derive(Clone)]
pub struct ValidatedClientInput {
    types: novarocks_type_contract::ValidatedClientInputTypes,
}
impl ValidatedClientInput {
    pub fn try_new(
        types: novarocks_type_contract::ValidatedClientInputTypes,
    ) -> Result<Self, RenderError> {
        let FrozenRootOutput::ClientRows(schema) = types.contract().output() else {
            return Err(error(E::SchemaMismatch));
        };
        for (ordinal, column) in schema.columns().iter().enumerate() {
            let binding = types
                .binding_at(column.source_ordinal as usize)
                .ok_or_else(|| error(E::SchemaMismatch))?;
            validate_field(&column.field, binding.data_type(), false).map_err(|mut error| {
                error.output_ordinal = Some(ordinal as u32);
                error
            })?;
        }
        Ok(Self { types })
    }

    pub fn contract(&self) -> &Arc<RootOutputContract> {
        self.types.contract()
    }

    pub fn is_for(
        &self,
        contract: &Arc<RootOutputContract>,
        input: &Arc<dyn novarocks_type_contract::ClientInputTypeBindings>,
    ) -> bool {
        self.types.is_for(contract, input)
    }

    pub fn rebind_shared(
        &self,
        input: Arc<dyn novarocks_type_contract::ClientInputTypeBindings>,
    ) -> Option<Self> {
        self.types.rebind_shared(input).map(|types| Self { types })
    }
}
impl ArrowClientRowEncoder {
    pub fn try_new(
        schema: Arc<ClientRenderSchema>,
        batch: RecordBatch,
    ) -> Result<Self, RenderError> {
        Self::try_new_with_owner(SchemaOwner::Standalone(schema), batch)
    }
    /// Reuse the plan's immutable schema owner without cloning its Vec/String
    /// backings when a BE root starts a new admitted input cursor.
    pub fn try_new_root(
        contract: Arc<RootOutputContract>,
        batch: RecordBatch,
    ) -> Result<Self, RenderError> {
        if !matches!(contract.output(), FrozenRootOutput::ClientRows(_)) {
            return Err(error(E::SchemaMismatch));
        }
        Self::try_new_with_owner(SchemaOwner::Root(contract), batch)
    }

    /// Keep the complete input types and their original creator responsibility
    /// independently of the input Chunk. Physical array validation remains
    /// separate from this previously established render-purpose proof.
    pub fn try_new_typed_root(
        input: ValidatedClientInput,
        batch: RecordBatch,
    ) -> Result<Self, RenderError> {
        Self::try_new_with_owner(SchemaOwner::TypedRoot(input), batch)
    }
    fn try_new_with_owner(schema: SchemaOwner, batch: RecordBatch) -> Result<Self, RenderError> {
        trino_json::validate_schema(&schema)?;
        if schema.columns().is_empty() || schema.columns().len() > V::MAX_COLUMNS {
            return Err(error(E::SchemaMismatch));
        }
        let typed = if let SchemaOwner::TypedRoot(input) = &schema {
            if !input.types.matches_schema(batch.schema_ref()) {
                return Err(error(E::SchemaMismatch));
            }
            true
        } else {
            false
        };
        if !typed {
            for (ordinal, column) in schema.columns().iter().enumerate() {
                let i = column.source_ordinal as usize;
                let field = batch
                    .schema()
                    .fields()
                    .get(i)
                    .cloned()
                    .ok_or_else(|| error(E::SchemaMismatch))?;
                if field.is_nullable() != column.field.nullable {
                    return Err(RenderError {
                        kind: E::SchemaMismatch,
                        output_ordinal: Some(ordinal as u32),
                        required: None,
                    });
                }
                validate_field(&column.field, field.data_type(), false).map_err(|mut e| {
                    e.output_ordinal = Some(ordinal as u32);
                    e
                })?;
            }
        }
        Ok(Self {
            schema,
            batch,
            stage: Box::new([0; V::SMALL_ROW_BYTES]),
            lengths: Box::new([0; V::MAX_COLUMNS]),
            cursor: CellCursor::new(),
            phase: Phase::Small,
            row: 0,
            column: 0,
            cell_started: false,
            cell_length: 0,
            row_length: 0,
            elements: 0,
            stage_len: 0,
            stage_at: 0,
            prefix_at: 0,
            cell_atom: Atom::new(),
        })
    }
    fn reset_row(&mut self, phase: Phase) {
        self.phase = phase;
        self.column = 0;
        self.cell_started = false;
        self.cell_length = 0;
        self.row_length = 0;
        self.elements = 0;
        self.stage_len = 0;
        self.stage_at = 0;
        self.prefix_at = 0;
        self.cursor.stack.clear();
    }
    fn start_cell(&mut self, budget: &mut Budget) -> Result<bool, RenderError> {
        if !budget.room(4096) || !budget.cell() {
            return Ok(false);
        }
        budget.scan(16);
        let column = &self.schema.columns()[self.column];
        let array = self.batch.column(column.source_ordinal as usize).clone();
        self.cell_started = true;
        self.cell_length = 0;
        self.cursor
            .reset(array, self.row, Path::root(self.column), self.elements);
        if self.column + 1 == self.schema.columns().len() {
            // The closing delimiter belongs to the last cell's measured payload.
            self.cursor
                .stack
                .insert(0, EncodeStep::Atom(Atom::literal(b"]")));
            budget.scan(size_of::<EncodeStep>());
        }
        self.cell_atom = if self.phase == Phase::Emit {
            Atom::literal(if self.column == 0 { b"[" } else { b"," })
        } else {
            Atom::new()
        };
        if self.phase == Phase::Small {
            if self.stage_len == V::SMALL_ROW_BYTES {
                self.reset_row(Phase::Count);
                return Ok(true);
            }
            self.stage[self.stage_len] = if self.column == 0 { b'[' } else { b',' };
            self.stage_len += 1;
            budget.scan(1);
        }
        Ok(true)
    }
    fn add_length(&mut self, n: usize) -> Result<(), RenderError> {
        self.row_length = self
            .row_length
            .checked_add(n)
            .ok_or_else(|| error(E::ArithmeticOverflow))?;
        if self.row_length > V::ROW_PAYLOAD_BYTES as usize {
            return Err(extent_error(E::RowTooLarge, self.row_length));
        }
        Ok(())
    }
    fn complete_cell(&mut self) -> Result<(), RenderError> {
        self.elements = self.cursor.elements;
        match self.phase {
            Phase::Small => {}
            Phase::Count => {
                self.lengths[self.column] = self.cell_length as u32;
                self.add_length(1 + self.cell_length)?;
            }
            Phase::Emit => {
                if self.cell_length != self.lengths[self.column] as usize {
                    return Err(error(E::SchemaMismatch));
                }
            }
            _ => return Err(invalid()),
        }
        self.column += 1;
        self.cell_started = false;
        self.cell_length = 0;
        Ok(())
    }
    fn run(&mut self, out: &mut [u8]) -> Result<RenderTurn, RenderError> {
        let mut b = Budget::new();
        let mut completed = 0u64;
        let mut status = RenderTurnStatus::Yielded;
        loop {
            if self.phase == Phase::Cancelled {
                return Err(error(E::Cancelled));
            }
            if self.phase == Phase::Failed {
                return Err(invalid());
            }
            if self.row == self.batch.num_rows() {
                self.phase = Phase::Complete;
                status = RenderTurnStatus::InputComplete;
                break;
            }
            if matches!(self.phase, Phase::ReadySmall | Phase::Emit) {
                if self.phase == Phase::ReadySmall && self.prefix_at == 0 {
                    if out.len() - b.emitted < 5 {
                        status = RenderTurnStatus::NeedsOutput;
                        break;
                    }
                    if !b.room(5) {
                        break;
                    }
                    out[b.emitted..b.emitted + 4]
                        .copy_from_slice(&(self.row_length as u32).to_le_bytes());
                    b.emit(4);
                    self.prefix_at = 4;
                }
                if self.phase == Phase::ReadySmall {
                    let n = (self.stage_len - self.stage_at)
                        .min(out.len() - b.emitted)
                        .min(V::EMIT_BYTES_PER_TURN - b.work);
                    if n == 0 {
                        status = if b.emitted == out.len() {
                            RenderTurnStatus::NeedsOutput
                        } else {
                            RenderTurnStatus::Yielded
                        };
                        break;
                    }
                    out[b.emitted..b.emitted + n]
                        .copy_from_slice(&self.stage[self.stage_at..self.stage_at + n]);
                    self.stage_at += n;
                    b.emit(n);
                    if self.stage_at == self.stage_len {
                        self.row += 1;
                        completed += 1;
                        self.reset_row(Phase::Small);
                    }
                    continue;
                }
            }
            if self.column == self.schema.columns().len() {
                match self.phase {
                    Phase::Small => {
                        self.row_length = self.stage_len;
                        self.phase = Phase::ReadySmall;
                        self.stage_at = 0;
                        self.prefix_at = 0;
                    }
                    Phase::Count => {
                        let total = self.row_length;
                        self.reset_row(Phase::Emit);
                        self.row_length = total;
                    }
                    Phase::Emit => {
                        self.row += 1;
                        completed += 1;
                        self.reset_row(Phase::Small);
                    }
                    _ => return Err(invalid()),
                }
                continue;
            }
            if self.phase == Phase::Emit
                && ((self.prefix_at == 0 && out.len() - b.emitted < 5) || b.emitted == out.len())
            {
                status = RenderTurnStatus::NeedsOutput;
                break;
            }
            if !b.room(256) {
                break;
            }
            if !self.cell_started {
                if !self.start_cell(&mut b)? {
                    break;
                }
                if !self.cell_started {
                    continue;
                }
            }
            if self.phase == Phase::Emit && self.cell_atom.at < self.cell_atom.len {
                let byte = self.cell_atom.data[self.cell_atom.at];
                self.cell_atom.at += 1;
                if self.prefix_at == 0 {
                    if !b.room(5) {
                        self.cell_atom.at -= 1;
                        break;
                    }
                    out[b.emitted..b.emitted + 4]
                        .copy_from_slice(&(self.row_length as u32).to_le_bytes());
                    b.emit(4);
                    self.prefix_at = 4;
                }
                if !b.room(1) {
                    self.cell_atom.at -= 1;
                    break;
                }
                out[b.emitted] = byte;
                b.emit(1);
                continue;
            }
            if self.phase == Phase::Small && self.stage_len == V::SMALL_ROW_BYTES {
                // Ask the cursor before classifying a row that ends exactly
                // at the staging boundary as a larger counted row.
                match self.cursor.next(&self.schema, &mut b)? {
                    Poll::Done => {
                        self.complete_cell()?;
                    }
                    Poll::Yield => break,
                    Poll::Byte(_) => self.reset_row(Phase::Count),
                }
                continue;
            }
            let n = match self.phase {
                Phase::Small => self.cursor.drain(
                    &mut b,
                    Some(&mut self.stage[self.stage_len..]),
                    V::SMALL_ROW_BYTES - self.stage_len,
                    false,
                )?,
                Phase::Count => self
                    .cursor
                    .drain(&mut b, None, V::EMIT_BYTES_PER_TURN, false)?,
                Phase::Emit => {
                    let at = b.emitted;
                    let capacity = out.len() - at;
                    self.cursor
                        .drain(&mut b, Some(&mut out[at..]), capacity, true)?
                }
                _ => 0,
            };
            if n != 0 {
                self.cell_length = self
                    .cell_length
                    .checked_add(n)
                    .ok_or_else(|| error(E::ArithmeticOverflow))?;
                if self.cell_length > V::ROW_PAYLOAD_BYTES as usize {
                    return Err(extent_error(E::RowTooLarge, self.cell_length));
                }
                match self.phase {
                    Phase::Small => {
                        self.stage_len += n;
                    }
                    Phase::Count | Phase::Emit => {}
                    _ => return Err(invalid()),
                }
                continue;
            }
            match self.cursor.next(&self.schema, &mut b)? {
                Poll::Yield => break,
                Poll::Done => {
                    self.complete_cell()?;
                }
                Poll::Byte(byte) => {
                    self.cell_length = self
                        .cell_length
                        .checked_add(1)
                        .ok_or_else(|| error(E::ArithmeticOverflow))?;
                    if self.cell_length > V::ROW_PAYLOAD_BYTES as usize {
                        return Err(extent_error(E::RowTooLarge, self.cell_length));
                    }
                    match self.phase {
                        Phase::Small => {
                            self.stage[self.stage_len] = byte;
                            self.stage_len += 1;
                            b.scan(1);
                        }
                        Phase::Count => b.scan(1),
                        Phase::Emit => {
                            out[b.emitted] = byte;
                            b.emit(1);
                        }
                        _ => return Err(invalid()),
                    }
                }
            }
        }
        Ok(RenderTurn {
            emitted_bytes: b.emitted,
            examined_bytes: b.examined,
            visited_cells: b.cells,
            completed_rows: completed,
            status,
        })
    }
}
impl BoundedClientRowEncoder for ArrowClientRowEncoder {
    fn step(&mut self, output: &mut [u8]) -> Result<RenderTurn, RenderError> {
        let length = output.len().min(V::SEGMENT_BYTES);
        let result = self.run(&mut output[..length]);
        if let Err(mut e) = result {
            if self.phase != Phase::Cancelled {
                self.phase = Phase::Failed;
            }
            e.output_ordinal = Some(self.column as u32);
            Err(e)
        } else {
            result
        }
    }
    fn cancel(&mut self) {
        self.phase = Phase::Cancelled;
        self.cursor.stack.clear();
    }
    fn scratch_capacity_bytes(&self) -> usize {
        size_of::<Self>()
            + size_of::<[u8; V::SMALL_ROW_BYTES]>()
            + size_of::<[u32; V::MAX_COLUMNS]>()
            + self.cursor.stack.capacity() * size_of::<EncodeStep>()
    }
}

/// All encoder allocations are fixed before the first turn. The stack refuses
/// an additional task before its original allocation could grow.
pub(super) const fn scratch_capacity_bound_bytes() -> usize {
    size_of::<ArrowClientRowEncoder>()
        + size_of::<[u8; V::SMALL_ROW_BYTES]>()
        + size_of::<[u32; V::MAX_COLUMNS]>()
        + CELL_TASK_CAPACITY * size_of::<EncodeStep>()
}

/// Trino JSON presentation over the shared bounded row machine.
#[path = "trino_json.rs"]
mod trino_json;
pub use trino_json::ArrowTrinoJsonEncoder;

#[cfg(test)]
#[path = "encoder_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "decimal256_legacy_contract_tests.rs"]
mod decimal256_legacy_contract_tests;
