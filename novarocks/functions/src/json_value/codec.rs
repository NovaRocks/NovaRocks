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

use super::{JsonValueError as Error, MAX_DEPTH, ObservedError, VERSION};
use novarocks_type_contract::{
    PICOS_PER_SECOND, TemporalMicros, TemporalPrecision, TemporalType, TemporalValue, TimeZoneKey,
};

const NULL: u8 = 1 << 5;
const SCALAR: u8 = 2 << 5;
pub(super) const STRING_TAG: u8 = SCALAR | (2 << 1);
const ARRAY: u8 = 3 << 5;
const OBJECT: u8 = 4 << 5;
const INDEX_ARRAY: u8 = 5 << 5;
const INDEX_OBJECT: u8 = 6 << 5;
const INDEX_THRESHOLD: usize = 8;

#[derive(Clone, Copy, Debug)]
pub enum Number<'a> {
    Finite { scale: i32, coefficient: &'a [u8] },
    NaN,
    PositiveInfinity,
    NegativeInfinity,
}

#[derive(Clone, Copy, Debug)]
pub enum Scalar<'a> {
    Boolean(bool),
    String(&'a str),
    TinyInt(i8),
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Real(f32),
    Double(f64),
    Decimal {
        precision: u8,
        scale: u8,
        coefficient: i128,
    },
    Number(Number<'a>),
    Date(i32),
    Temporal {
        ty: TemporalType,
        value: TemporalValue,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum Kind<'a> {
    Null,
    Scalar(Scalar<'a>),
    Array(Container<'a>),
    Object(Container<'a>),
}

/// An item borrowed from an admitted value. Child boundaries are checked
/// when those children are traversed.
#[derive(Clone, Copy, Debug)]
pub struct Node<'a>(&'a [u8]);

impl<'a> Node<'a> {
    pub fn from_root(bytes: &'a [u8]) -> Result<Self, Error> {
        let (version, bytes) = bytes.split_first().ok_or(Error::Version)?;
        if *version != VERSION {
            return Err(Error::Version);
        }
        Self::exact(bytes)
    }

    pub(super) fn exact(bytes: &'a [u8]) -> Result<Self, Error> {
        let (node, rest) = Self::split(bytes)?;
        if rest.is_empty() {
            Ok(node)
        } else {
            Err(Error::Extent)
        }
    }

    fn split(bytes: &'a [u8]) -> Result<(Self, &'a [u8]), Error> {
        let (tag, body) = bytes.split_first().ok_or(Error::Extent)?;
        let mut payload = Reader(body);
        match *tag {
            NULL => {}
            tag if tag & 0xe1 == SCALAR => match (tag >> 1) & 15 {
                1 | 3 => {
                    payload.take(1)?;
                }
                2 => {
                    payload.blob()?;
                }
                4 => {
                    payload.take(2)?;
                }
                5 | 7 | 11 => {
                    payload.take(4)?;
                }
                6 | 8 => {
                    payload.take(8)?;
                }
                9 => {
                    payload.take(2)?;
                    payload.blob()?;
                }
                10 => match payload.byte()? {
                    0 => {
                        payload.take(4)?;
                        payload.blob()?;
                    }
                    1..=3 => {}
                    _ => return Err(Error::Scalar),
                },
                12 => {
                    payload.take(9)?;
                }
                13 => {
                    payload.take(13)?;
                }
                14 => {
                    payload.take(17)?;
                }
                15 => {
                    payload.take(17)?;
                    payload.blob()?;
                }
                _ => return Err(Error::Tag),
            },
            tag if container_kind(tag).is_some() => {
                payload.blob()?;
            }
            _ => return Err(Error::Tag),
        }
        let size = bytes.len() - payload.0.len();
        Ok((Self(&bytes[..size]), payload.0))
    }

    pub(super) fn string_bytes(self) -> Result<Option<&'a [u8]>, Error> {
        if self.0[0] != STRING_TAG {
            return Ok(None);
        }
        Ok(Some(Reader(&self.0[1..]).blob()?))
    }

    pub fn item_bytes(self) -> &'a [u8] {
        self.0
    }

    pub fn kind(self) -> Result<Kind<'a>, Error> {
        let tag = self.0[0];
        if tag == NULL {
            return Ok(Kind::Null);
        }
        if let Some(object) = container_kind(tag) {
            let container = Container::read(tag, &self.0[1..], object)?;
            return Ok(if object {
                Kind::Object(container)
            } else {
                Kind::Array(container)
            });
        }
        let mut payload = Reader(&self.0[1..]);
        Ok(Kind::Scalar(payload.scalar((tag >> 1) & 15)?))
    }

    /// Check the accessed payload and observe long UTF-8 scalar inputs.
    pub fn kind_observed<E>(
        self,
        mut work: impl FnMut() -> Result<(), E>,
    ) -> Result<Kind<'a>, ObservedError<E>> {
        work().map_err(ObservedError::Control)?;
        if let Some(bytes) = self.string_bytes()? {
            validate_utf8(bytes, &mut work)?;
            // SAFETY: validate_utf8 checks the entire same immutable slice
            // with complete Unicode scalar boundaries across its chunks.
            let text = unsafe { std::str::from_utf8_unchecked(bytes) };
            Ok(Kind::Scalar(Scalar::String(text)))
        } else {
            Ok(self.kind()?)
        }
    }

    pub fn validate<E>(
        self,
        mut work: impl FnMut() -> Result<(), E>,
    ) -> Result<(), ObservedError<E>> {
        if container_kind(self.0[0]).is_some() {
            self.walk(true, work).map(|_| ())
        } else {
            work().map_err(ObservedError::Control)?;
            self.validate_leaf(&mut work)
        }
    }

    /// Measures the container depth of an already-admitted value. This checks
    /// reachable boundaries without revalidating keys and scalar payloads.
    pub fn depth<E>(
        self,
        mut work: impl FnMut() -> Result<(), E>,
    ) -> Result<usize, ObservedError<E>> {
        if container_kind(self.0[0]).is_some() {
            self.walk(false, work)
        } else {
            work().map_err(ObservedError::Control)?;
            Ok(0)
        }
    }

    fn validate_leaf<E>(
        self,
        work: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<(), ObservedError<E>> {
        if let Some(bytes) = self.string_bytes()? {
            validate_utf8(bytes, work)?;
        } else {
            self.kind()?;
        }
        Ok(())
    }

    fn walk<E>(
        self,
        validate: bool,
        mut work: impl FnMut() -> Result<(), E>,
    ) -> Result<usize, ObservedError<E>> {
        struct Frame<'a> {
            container: Container<'a>,
            index: usize,
            consumed: usize,
        }
        // A fixed stack keeps package decode free of per-value allocation.
        // Its extent follows the Trino document's actual container depth bound.
        let mut stack: [Option<Frame<'_>>; MAX_DEPTH] = [const { None }; MAX_DEPTH];
        let mut depth = 0;
        let mut maximum = 0;
        let mut current = self;
        loop {
            work().map_err(ObservedError::Control)?;
            let tag = current.0[0];
            if let Some(object) = container_kind(tag) {
                if depth == MAX_DEPTH {
                    return Err(Error::Depth.into());
                }
                let container = Container::read(tag, &current.0[1..], object)?;
                if validate {
                    container.validate_directory(&mut work)?;
                }
                stack[depth] = Some(Frame {
                    container,
                    index: 0,
                    consumed: 0,
                });
                depth += 1;
                maximum = maximum.max(depth);
            } else if validate {
                current.validate_leaf(&mut work)?;
            }
            loop {
                if depth == 0 {
                    return Ok(maximum);
                }
                work().map_err(ObservedError::Control)?;
                let frame = stack[depth - 1].as_mut().expect("active frame");
                if let Some((key, value, consumed)) =
                    frame.container.raw_next(frame.index, frame.consumed)?
                {
                    if let (true, Some(key)) = (validate, key) {
                        validate_utf8(key, &mut work)?;
                    }
                    frame.index += 1;
                    frame.consumed = consumed;
                    current = value;
                    break;
                }
                stack[depth - 1] = None;
                depth -= 1;
            }
        }
    }
}

fn container_kind(tag: u8) -> Option<bool> {
    match tag {
        ARRAY => Some(false),
        OBJECT => Some(true),
        tag if tag & 0xe7 == INDEX_ARRAY => Some(false),
        tag if tag & 0xe1 == INDEX_OBJECT => Some(true),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Container<'a> {
    count: usize,
    object: bool,
    data: &'a [u8],
    offsets: &'a [u8],
    ordinals: &'a [u8],
    offset_width: usize,
    ordinal_width: usize,
}

impl<'a> Container<'a> {
    fn read(tag: u8, payload: &'a [u8], object: bool) -> Result<Self, Error> {
        let body = Reader(payload).blob()?;
        let mut cursor = Reader(body);
        let count = cursor.len()?;
        let indexed = tag >= INDEX_ARRAY;
        if indexed != (count >= INDEX_THRESHOLD) {
            return Err(Error::Directory);
        }
        let (offsets, ordinals, offset_width, ordinal_width) = if indexed {
            let offset_width = 1 << ((tag >> 3) & 3);
            let ordinal_width = if object { 1 << ((tag >> 1) & 3) } else { 0 };
            let offsets = cursor.take(
                count
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(offset_width))
                    .ok_or(Error::Extent)?,
            )?;
            let ordinals = cursor.take(count.checked_mul(ordinal_width).ok_or(Error::Extent)?)?;
            if width(cursor.0.len() as u64) != offset_width
                || (object && width((count - 1) as u64) != ordinal_width)
                || read_width(offsets, offset_width, 0)? != 0
                || read_width(offsets, offset_width, count)? != cursor.0.len()
            {
                return Err(Error::Directory);
            }
            (offsets, ordinals, offset_width, ordinal_width)
        } else {
            (&[][..], &[][..], 0, 0)
        };
        let minimum = if object { 2 } else { 1 };
        if count > cursor.0.len() / minimum {
            return Err(Error::Extent);
        }
        Ok(Self {
            count,
            object,
            data: cursor.0,
            offsets,
            ordinals,
            offset_width,
            ordinal_width,
        })
    }

    pub fn len(self) -> usize {
        self.count
    }

    pub fn is_empty(self) -> bool {
        self.count == 0
    }

    fn member(self, index: usize) -> Result<&'a [u8], Error> {
        let begin = read_width(self.offsets, self.offset_width, index)?;
        let end = read_width(self.offsets, self.offset_width, index + 1)?;
        let data = self.data.get(begin..end).ok_or(Error::Directory)?;
        if data.is_empty() {
            return Err(Error::Directory);
        }
        Ok(data)
    }

    fn indexed_entry(self, index: usize) -> Result<Entry<'a>, Error> {
        let mut cursor = Reader(self.member(index)?);
        let key = if self.object {
            Some(cursor.text()?)
        } else {
            None
        };
        Ok(Entry {
            key,
            value: Node::exact(cursor.0)?,
        })
    }

    /// Tests adjacent original keys in the ordered object directory.
    pub(crate) fn has_unique_keys_observed<E>(
        self,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<bool, ObservedError<E>> {
        if !self.object {
            return Err(Error::Tag.into());
        }
        if self.offset_width != 0 {
            let mut previous = None;
            for index in 0..self.count {
                observe().map_err(ObservedError::Control)?;
                let ordinal = self.ordinal(index)?;
                let entry = self.indexed_entry(ordinal)?;
                let key = entry.key.ok_or(Error::Directory)?;
                if let Some(previous) = previous
                    && equal_keys(previous, key, observe)?
                {
                    return Ok(false);
                }
                previous = Some(key);
            }
            return Ok(true);
        }
        for (index, result) in self.entries().enumerate() {
            observe().map_err(ObservedError::Control)?;
            let key = result?.key.ok_or(Error::Directory)?;
            for prior in self.entries().take(index) {
                observe().map_err(ObservedError::Control)?;
                if equal_keys(prior?.key.ok_or(Error::Directory)?, key, observe)? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    pub fn get(self, index: usize) -> Result<Option<Entry<'a>>, Error> {
        if index >= self.count {
            return Ok(None);
        }
        if self.offset_width != 0 {
            return self.indexed_entry(index).map(Some);
        }
        let mut result = None;
        for entry in self.entries().take(index + 1) {
            result = Some(entry?);
        }
        Ok(result)
    }

    /// Direct member-order traversal, retaining a resumable position. The
    /// renderer validates keys incrementally as it writes escaped UTF-8.
    pub(super) fn raw_next(
        self,
        index: usize,
        consumed: usize,
    ) -> Result<Option<(Option<&'a [u8]>, Node<'a>, usize)>, Error> {
        let original = self.data.get(consumed..).ok_or(Error::Extent)?;
        if index == self.count {
            return if original.is_empty() {
                Ok(None)
            } else {
                Err(Error::Extent)
            };
        }
        if index > self.count {
            return Err(Error::Extent);
        }
        let bounded = if self.offset_width == 0 {
            original
        } else {
            let member = self.member(index)?;
            if !std::ptr::eq(member.as_ptr(), original.as_ptr()) {
                return Err(Error::Directory);
            }
            member
        };
        let mut cursor = Reader(bounded);
        let key = if self.object {
            Some(cursor.blob()?)
        } else {
            None
        };
        let (value, rest) = Node::split(cursor.0)?;
        if self.offset_width != 0 && !rest.is_empty() {
            return Err(Error::Directory);
        }
        Ok(Some((key, value, consumed + bounded.len() - rest.len())))
    }

    pub fn entries(self) -> Entries<'a> {
        Entries {
            container: self,
            remaining: self.data,
            index: 0,
        }
    }

    pub fn lookup<'k>(self, key: &'k str) -> Result<Lookup<'a, 'k>, Error> {
        if !self.object {
            return Err(Error::Tag);
        }
        if self.offset_width == 0 {
            return Ok(Lookup {
                key,
                entries: self.entries(),
                positions: None,
            });
        }
        let mut low = 0;
        let mut high = self.count;
        while low < high {
            let middle = low + (high - low) / 2;
            if self.lookup_entry(middle)?.key.ok_or(Error::Directory)? < key {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(Lookup {
            key,
            entries: self.entries(),
            positions: Some(low..self.count),
        })
    }

    fn ordinal(self, index: usize) -> Result<usize, Error> {
        let ordinal = read_width(self.ordinals, self.ordinal_width, index)?;
        if ordinal >= self.count {
            return Err(Error::Directory);
        }
        Ok(ordinal)
    }

    fn lookup_entry(self, index: usize) -> Result<Entry<'a>, Error> {
        self.indexed_entry(self.ordinal(index)?)
    }

    fn validate_directory<E>(
        self,
        work: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<(), ObservedError<E>> {
        if self.ordinals.is_empty() {
            return Ok(());
        }
        let mut previous: Option<(&[u8], usize)> = None;
        for i in 0..self.count {
            work().map_err(ObservedError::Control)?;
            let ordinal = self.ordinal(i)?;
            let key = Reader(self.member(ordinal)?).blob()?;
            if let Some(old) = previous {
                let mut order = std::cmp::Ordering::Equal;
                for (left, right) in old.0.chunks(256).zip(key.chunks(256)) {
                    work().map_err(ObservedError::Control)?;
                    order = left.cmp(right);
                    if !order.is_eq() {
                        break;
                    }
                }
                if order
                    .then_with(|| old.0.len().cmp(&key.len()))
                    .then_with(|| old.1.cmp(&ordinal))
                    != std::cmp::Ordering::Less
                {
                    return Err(Error::Directory.into());
                }
            }
            previous = Some((key, ordinal));
        }
        Ok(())
    }
}

pub(crate) fn validate_utf8<E>(
    mut bytes: &[u8],
    work: &mut impl FnMut() -> Result<(), E>,
) -> Result<(), ObservedError<E>> {
    while !bytes.is_empty() {
        work().map_err(ObservedError::Control)?;
        let len = bytes.len().min(256);
        match std::str::from_utf8(&bytes[..len]) {
            Ok(_) => bytes = &bytes[len..],
            Err(error) if error.error_len().is_none() && len != bytes.len() => {
                bytes = &bytes[error.valid_up_to()..];
            }
            Err(_) => return Err(Error::Text.into()),
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub struct Entry<'a> {
    pub key: Option<&'a str>,
    pub value: Node<'a>,
}

#[derive(Clone, Debug)]
pub struct Entries<'a> {
    container: Container<'a>,
    remaining: &'a [u8],
    index: usize,
}

impl<'a> Iterator for Entries<'a> {
    type Item = Result<Entry<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index > self.container.count {
            return None;
        }
        let consumed = self.container.data.len() - self.remaining.len();
        let result: Result<Option<_>, Error> = (|| {
            let Some((key, value, consumed)) = self.container.raw_next(self.index, consumed)?
            else {
                return Ok(None);
            };
            let key = key
                .map(std::str::from_utf8)
                .transpose()
                .map_err(|_| Error::Text)?;
            self.remaining = &self.container.data[consumed..];
            Ok(Some(Entry { key, value }))
        })();
        self.index += 1;
        if result.is_err() {
            self.index = self.container.count + 1;
        }
        result.transpose()
    }
}

pub struct Lookup<'a, 'k> {
    key: &'k str,
    entries: Entries<'a>,
    positions: Option<std::ops::Range<usize>>,
}

impl<'a> Iterator for Lookup<'a, '_> {
    type Item = Result<Node<'a>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(positions) = &mut self.positions {
            let item = self.entries.container.lookup_entry(positions.next()?);
            return match item {
                Ok(entry) if entry.key == Some(self.key) => Some(Ok(entry.value)),
                Ok(_) => {
                    positions.start = positions.end;
                    None
                }
                Err(error) => {
                    positions.start = positions.end;
                    Some(Err(error))
                }
            };
        }
        self.entries.find_map(|entry| match entry {
            Ok(entry) if entry.key == Some(self.key) => Some(Ok(entry.value)),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
    }
}

fn equal_keys<E>(
    first: &str,
    second: &str,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<bool, ObservedError<E>> {
    if first.len() != second.len() {
        return Ok(false);
    }
    for (first, second) in first
        .as_bytes()
        .chunks(256)
        .zip(second.as_bytes().chunks(256))
    {
        observe().map_err(ObservedError::Control)?;
        if first != second {
            return Ok(false);
        }
    }
    Ok(true)
}

#[derive(Clone, Copy)]
pub(super) struct Reader<'a>(pub(super) &'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, size: usize) -> Result<&'a [u8], Error> {
        let (value, tail) = self.0.split_at_checked(size).ok_or(Error::Extent)?;
        self.0 = tail;
        Ok(value)
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        Ok(self.take(N)?.try_into().expect("fixed size slice"))
    }

    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    fn len(&mut self) -> Result<usize, Error> {
        let mut value = 0_u64;
        for shift in (0..64).step_by(7) {
            let byte = self.byte()?;
            if shift == 63 && byte > 1 {
                return Err(Error::Extent);
            }
            value |= u64::from(byte & 127) << shift;
            if byte < 128 {
                if shift != 0 && byte == 0 {
                    return Err(Error::Extent);
                }
                return usize::try_from(value).map_err(|_| Error::Extent);
            }
        }
        Err(Error::Extent)
    }

    pub(super) fn blob(&mut self) -> Result<&'a [u8], Error> {
        let size = self.len()?;
        self.take(size)
    }

    fn text(&mut self) -> Result<&'a str, Error> {
        std::str::from_utf8(self.blob()?).map_err(|_| Error::Text)
    }

    fn scalar(&mut self, subtag: u8) -> Result<Scalar<'a>, Error> {
        Ok(match subtag {
            1 => match self.byte()? {
                0 => Scalar::Boolean(false),
                1 => Scalar::Boolean(true),
                _ => return Err(Error::Scalar),
            },
            2 => Scalar::String(self.text()?),
            3 => Scalar::TinyInt(i8::from_le_bytes(self.fixed()?)),
            4 => Scalar::SmallInt(i16::from_le_bytes(self.fixed()?)),
            5 => Scalar::Integer(i32::from_le_bytes(self.fixed()?)),
            6 => Scalar::BigInt(i64::from_le_bytes(self.fixed()?)),
            7 => Scalar::Real(f32::from_le_bytes(self.fixed()?)),
            8 => Scalar::Double(f64::from_le_bytes(self.fixed()?)),
            9 => {
                let precision = self.byte()?;
                let scale = self.byte()?;
                let bytes = self.blob()?;
                let coefficient = super::number::read_i128(bytes)?;
                validate_decimal(precision, scale, coefficient)?;
                Scalar::Decimal {
                    precision,
                    scale,
                    coefficient,
                }
            }
            10 => Scalar::Number(match self.byte()? {
                0 => {
                    let scale = i32::from_le_bytes(self.fixed()?);
                    let coefficient = self.blob()?;
                    super::number::validate_number(scale, coefficient)?;
                    Number::Finite { scale, coefficient }
                }
                1 => Number::NaN,
                2 => Number::PositiveInfinity,
                3 => Number::NegativeInfinity,
                _ => return Err(Error::Scalar),
            }),
            11 => Scalar::Date(i32::from_le_bytes(self.fixed()?)),
            12..=15 => {
                let precision = TemporalPrecision::new(self.byte()?).map_err(|_| Error::Scalar)?;
                let (ty, value) = match subtag {
                    12 => (
                        TemporalType::Time(precision),
                        TemporalValue::Time(self.clock()?),
                    ),
                    13 => {
                        let picos = self.clock()?;
                        let offset = i32::from_le_bytes(self.fixed()?);
                        let offset_minutes = i16::try_from(offset).map_err(|_| Error::Scalar)?;
                        (
                            TemporalType::TimeWithTimeZone(precision),
                            TemporalValue::TimeWithTimeZone {
                                picos,
                                offset_minutes,
                            },
                        )
                    }
                    14 => (
                        TemporalType::Timestamp(precision),
                        TemporalValue::Timestamp(self.timestamp()?),
                    ),
                    _ => {
                        let instant = self.timestamp()?;
                        let name = self.text()?;
                        let zone = TimeZoneKey::parse(name)
                            .filter(|zone| zone.name() == name)
                            .ok_or(Error::Scalar)?;
                        (
                            TemporalType::TimestampWithTimeZone(precision),
                            TemporalValue::TimestampWithTimeZone { instant, zone },
                        )
                    }
                };
                value.validate(ty).map_err(|_| Error::Scalar)?;
                Scalar::Temporal { ty, value }
            }
            _ => return Err(Error::Tag),
        })
    }

    fn clock(&mut self) -> Result<i64, Error> {
        i64::try_from(u64::from_le_bytes(self.fixed()?)).map_err(|_| Error::Scalar)
    }

    fn timestamp(&mut self) -> Result<TemporalMicros, Error> {
        let seconds = i64::from_le_bytes(self.fixed()?);
        let picos = u64::from_le_bytes(self.fixed()?);
        if picos >= PICOS_PER_SECOND as u64 {
            return Err(Error::Scalar);
        }
        TemporalMicros::from_picos(
            i128::from(seconds) * i128::from(PICOS_PER_SECOND) + i128::from(picos),
        )
        .ok_or(Error::Scalar)
    }
}

fn validate_decimal(precision: u8, scale: u8, coefficient: i128) -> Result<(), Error> {
    if !(1..=38).contains(&precision)
        || scale > precision
        || coefficient.unsigned_abs() >= 10_u128.pow(u32::from(precision))
    {
        Err(Error::Scalar)
    } else {
        Ok(())
    }
}

pub(super) fn width(value: u64) -> usize {
    match value {
        0..=0xff => 1,
        0x100..=0xffff => 2,
        0x10000..=0xffffffff => 4,
        _ => 8,
    }
}

pub(super) fn read_width(bytes: &[u8], width: usize, index: usize) -> Result<usize, Error> {
    let offset = index.checked_mul(width).ok_or(Error::Extent)?;
    let tail = bytes.get(offset..).ok_or(Error::Directory)?;
    let bytes = tail.get(..width).ok_or(Error::Directory)?;
    let mut raw = [0; 8];
    raw[..width].copy_from_slice(bytes);
    usize::try_from(u64::from_le_bytes(raw)).map_err(|_| Error::Extent)
}

pub(super) fn leb_size(value: usize) -> usize {
    ((usize::BITS - value.leading_zeros()).max(1) as usize).div_ceil(7)
}

pub(super) fn write_leb(dst: &mut [u8], mut value: usize) -> usize {
    let length = leb_size(value);
    for byte in &mut dst[..length - 1] {
        *byte = value as u8 & 127 | 128;
        value >>= 7;
    }
    dst[length - 1] = value as u8;
    length
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Layout {
    pub tag: u8,
    pub count: usize,
    pub size: usize,
    pub body_size: usize,
    pub offset_width: usize,
    pub ordinal_width: usize,
}

impl Layout {
    pub fn new(object: bool, count: usize, members: usize) -> Result<Self, Error> {
        let (tag, offset_width, ordinal_width) = if count < INDEX_THRESHOLD {
            (if object { OBJECT } else { ARRAY }, 0, 0)
        } else {
            let offset = width(members as u64);
            let ordinal = if object { width((count - 1) as u64) } else { 0 };
            let tag = if object {
                INDEX_OBJECT | (ordinal.ilog2() as u8) << 1
            } else {
                INDEX_ARRAY
            };
            (tag | (offset.ilog2() as u8) << 3, offset, ordinal)
        };
        let directory = count
            .checked_mul(offset_width + ordinal_width)
            .and_then(|bytes| bytes.checked_add(offset_width))
            .ok_or(Error::Extent)?;
        let body_size = leb_size(count)
            .checked_add(directory)
            .and_then(|bytes| bytes.checked_add(members))
            .ok_or(Error::Extent)?;
        let size = (1 + leb_size(body_size))
            .checked_add(body_size)
            .ok_or(Error::Extent)?;
        Ok(Self {
            tag,
            count,
            size,
            body_size,
            offset_width,
            ordinal_width,
        })
    }

    pub fn write_header(self, out: &mut [u8]) -> (usize, usize, usize) {
        out[0] = self.tag;
        let count = 1 + write_leb(&mut out[1..], self.body_size);
        let offsets = count + write_leb(&mut out[count..], self.count);
        let ordinals = offsets + (self.count + 1) * self.offset_width;
        let members = ordinals + self.count * self.ordinal_width;
        (offsets, ordinals, members)
    }
}

pub(super) fn write_width(dst: &mut [u8], width: usize, position: usize, value: usize) {
    let begin = position * width;
    dst[begin..begin + width].copy_from_slice(&(value as u64).to_le_bytes()[..width]);
}

pub(super) const NULL_TAG: u8 = NULL;

pub(super) fn write_scalar(scalar: Scalar<'_>, out: &mut [u8]) -> Result<usize, Error> {
    let (tag, size) = scalar_layout(scalar)?;
    out[0] = SCALAR | (tag << 1);
    let dst = &mut out[1..size];
    match scalar {
        Scalar::Boolean(value) => dst[0] = u8::from(value),
        Scalar::String(value) => write_blob(dst, value.as_bytes()),
        Scalar::TinyInt(value) => dst.copy_from_slice(&value.to_le_bytes()),
        Scalar::SmallInt(value) => dst.copy_from_slice(&value.to_le_bytes()),
        Scalar::Integer(value) | Scalar::Date(value) => dst.copy_from_slice(&value.to_le_bytes()),
        Scalar::BigInt(value) => dst.copy_from_slice(&value.to_le_bytes()),
        Scalar::Real(value) => dst.copy_from_slice(&value.to_le_bytes()),
        Scalar::Double(value) => dst.copy_from_slice(&value.to_le_bytes()),
        Scalar::Decimal {
            precision,
            scale,
            coefficient,
        } => {
            dst[0] = precision;
            dst[1] = scale;
            let raw = coefficient.to_le_bytes();
            write_blob(&mut dst[2..], super::number::minimal_signed(&raw));
        }
        Scalar::Number(Number::Finite { scale, coefficient }) => {
            dst[0] = 0;
            dst[1..5].copy_from_slice(&scale.to_le_bytes());
            write_blob(&mut dst[5..], coefficient);
        }
        Scalar::Number(special) => {
            dst[0] = match special {
                Number::NaN => 1,
                Number::PositiveInfinity => 2,
                Number::NegativeInfinity => 3,
                Number::Finite { .. } => unreachable!(),
            }
        }
        Scalar::Temporal { ty, value } => {
            dst[0] = ty.precision().digits();
            match value {
                TemporalValue::Time(picos) => {
                    dst[1..9].copy_from_slice(&(picos as u64).to_le_bytes())
                }
                TemporalValue::TimeWithTimeZone {
                    picos,
                    offset_minutes,
                } => {
                    dst[1..9].copy_from_slice(&(picos as u64).to_le_bytes());
                    dst[9..13].copy_from_slice(&i32::from(offset_minutes).to_le_bytes());
                }
                TemporalValue::Timestamp(value) => write_timestamp(&mut dst[1..17], value),
                TemporalValue::TimestampWithTimeZone { instant, zone } => {
                    write_timestamp(&mut dst[1..17], instant);
                    write_blob(&mut dst[17..], zone.name().as_bytes());
                }
                _ => return Err(Error::Scalar),
            }
        }
    }
    Ok(size)
}

fn write_timestamp(dst: &mut [u8], value: TemporalMicros) {
    let picos = value.as_picos();
    let second = i128::from(PICOS_PER_SECOND);
    dst[..8].copy_from_slice(&(picos.div_euclid(second) as i64).to_le_bytes());
    dst[8..].copy_from_slice(&(picos.rem_euclid(second) as u64).to_le_bytes());
}

fn write_blob(dst: &mut [u8], bytes: &[u8]) {
    let header = write_leb(dst, bytes.len());
    dst[header..].copy_from_slice(bytes);
}

pub(super) fn scalar_layout(scalar: Scalar<'_>) -> Result<(u8, usize), Error> {
    let (tag, size) = match scalar {
        Scalar::Boolean(_) => (1, 1),
        Scalar::String(value) => (
            2,
            leb_size(value.len())
                .checked_add(value.len())
                .ok_or(Error::Extent)?,
        ),
        Scalar::TinyInt(_) => (3, 1),
        Scalar::SmallInt(_) => (4, 2),
        Scalar::Integer(_) => (5, 4),
        Scalar::BigInt(_) => (6, 8),
        Scalar::Real(_) => (7, 4),
        Scalar::Double(_) => (8, 8),
        Scalar::Decimal {
            precision,
            scale,
            coefficient,
        } => {
            validate_decimal(precision, scale, coefficient)?;
            let raw = coefficient.to_le_bytes();
            let len = super::number::minimal_signed(&raw).len();
            (9, 2 + leb_size(len) + len)
        }
        Scalar::Number(Number::Finite { scale, coefficient }) => {
            super::number::validate_number(scale, coefficient)?;
            (10, 5 + leb_size(coefficient.len()) + coefficient.len())
        }
        Scalar::Number(_) => (10, 1),
        Scalar::Date(_) => (11, 4),
        Scalar::Temporal { ty, value } => {
            value.validate(ty).map_err(|_| Error::Scalar)?;
            match value {
                TemporalValue::Time(_) => (12, 9),
                TemporalValue::TimeWithTimeZone { .. } => (13, 13),
                TemporalValue::Timestamp(_) => (14, 17),
                TemporalValue::TimestampWithTimeZone { zone, .. } => {
                    (15, 17 + leb_size(zone.name().len()) + zone.name().len())
                }
                _ => return Err(Error::Scalar),
            }
        }
    };
    Ok((tag, size.checked_add(1).ok_or(Error::Extent)?))
}
