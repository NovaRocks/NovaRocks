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

//! Trino container-to-JSON CAST walks actual selected Arrow members. The
//! source is stable across the sizing and encoding passes.

use super::{CastPrepareError, CompileCheckpoints, FunctionValueType, KernelFailure};
use crate::builtin::json_sql::read_scalar_value_type;
use crate::builtin::json_sql_selected::RowWork;
use crate::json_value::build::{Event, Scan, Source, encode_scan};
use crate::json_value::{FixedText, JsonError, JsonWork, Node, Scalar, write_scalar_text};
use crate::kernel_control::invalid;
use crate::opaque_memory::OpaqueReservation;
use arrow_array::{Array, LargeBinaryArray, ListArray, MapArray, StructArray};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CharacterKind, TemporalType, ValueField, ValueKind, ValueLogicalType, ValueTypeRef,
};
use std::cmp::Ordering;
use std::ops::Range;

/// Use the frozen SQL declaration for shape, ROW names and key eligibility.
pub(super) fn supports_cast(
    ty: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, CastPrepareError> {
    work.step()?;
    Ok(match ty.value_type().kind() {
        ValueKind::List(_) if matches!(ty.data_type(), DataType::List(_)) => {
            supports_cast(&child(ty, 0)?, work)?
        }
        ValueKind::Map { key, .. } => {
            map_key_kind(key.kind()) && supports_cast(&child(ty, 1)?, work)?
        }
        ValueKind::Struct(fields) => {
            for index in 0..fields.len() {
                if !supports_cast(&child(ty, index)?, work)? {
                    return Ok(false);
                }
            }
            true
        }
        ValueKind::Nominal(ValueLogicalType::Json) | ValueKind::Scalar(DataType::Date32) => true,
        ValueKind::Temporal(TemporalType::Timestamp(_)) => true,
        other => map_key_kind(other),
    })
}

fn child(ty: &FunctionValueType, index: usize) -> Result<FunctionValueType, CastPrepareError> {
    ty.child_at(index).ok_or(CastPrepareError::TypeMismatch)
}

fn map_key_kind(ty: &ValueKind) -> bool {
    match ty {
        ValueKind::String
        | ValueKind::Scalar(
            DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64,
        ) => true,
        ValueKind::Scalar(DataType::Decimal128(precision, scale)) => {
            (1..=38).contains(precision) && *scale >= 0 && (*scale as u8) <= *precision
        }
        ValueKind::BoundedCharacter(character) => character.kind() == CharacterKind::Varchar,
        _ => false,
    }
}

pub(super) fn encode_selected<'a>(
    array: &'a dyn Array,
    ty: &'a FunctionValueType,
    row: usize,
    work: &mut RowWork<'_, '_>,
) -> Result<Vec<u8>, JsonError> {
    let root = Cell {
        array,
        ty: ty.value_type(),
        row,
    };
    encode_scan(work, |work| SourceScan::new(root, work))
}

#[derive(Clone, Copy)]
struct Cell<'a> {
    array: &'a dyn Array,
    ty: &'a ValueTypeRef,
    row: usize,
}

impl<'a> Cell<'a> {
    fn concrete<T: Array + 'static>(self) -> Result<&'a T, KernelFailure> {
        self.array.as_any().downcast_ref::<T>().ok_or_else(|| {
            invalid("JSON CAST selected member differs from the declared Arrow layout")
        })
    }
    fn scalar(self) -> Result<Option<Scalar<'a>>, KernelFailure> {
        read_scalar_value_type(self.array, self.ty, self.row)
    }
}

enum Frame<'a> {
    Array {
        values: &'a dyn Array,
        ty: &'a ValueTypeRef,
        remaining: Range<usize>,
    },
    Row {
        array: &'a StructArray,
        fields: &'a [ValueField],
        row: usize,
        next: usize,
    },
    Map {
        values: &'a dyn Array,
        ty: &'a ValueTypeRef,
        keys: Vec<MapMember<'a>>,
        next: usize,
        _key_reservation: OpaqueReservation,
    },
}

enum MapKey<'a> {
    Borrowed(&'a str),
    Inline(FixedText<96>),
}
impl MapKey<'_> {
    fn as_str(&self) -> &str {
        match self {
            Self::Borrowed(text) => text,
            Self::Inline(text) => text.as_str(),
        }
    }
}
struct MapMember<'a> {
    key: MapKey<'a>,
    row: usize,
}

struct SourceScan<'a> {
    next: Option<Cell<'a>>,
    frames: Vec<Frame<'a>>,
    _frame_reservation: Option<OpaqueReservation>,
}

impl<'a> SourceScan<'a> {
    fn new(root: Cell<'a>, work: &mut RowWork<'_, '_>) -> Result<Self, JsonError> {
        let levels = if matches!(
            root.ty.kind(),
            ValueKind::List(_) | ValueKind::Struct(_) | ValueKind::Map { .. }
        ) {
            root.ty.depth()
        } else {
            0
        };
        let mut frames = Vec::new();
        let reservation = if levels > 0 {
            let grant = work.reserve(checked_extent::<Frame<'_>>(levels)?)?;
            frames
                .try_reserve_exact(levels)
                .map_err(|_| KernelFailure::ResourceExhausted)?;
            Some(grant)
        } else {
            None
        };
        Ok(Self {
            next: Some(root),
            frames,
            _frame_reservation: reservation,
        })
    }

    fn item(&mut self, item: Cell<'a>, work: &mut RowWork<'_, '_>) -> Result<Event<'a>, JsonError> {
        if item.row >= item.array.len() {
            return Err(invalid("JSON CAST row is beyond its source Arrow array").into());
        }
        if item.array.is_null(item.row) {
            return if item.ty.nullable() {
                Ok(Event::Item(Source::Null))
            } else {
                Err(invalid("non-null JSON CAST member contains an SQL NULL").into())
            };
        }
        let (object, frame) = match item.ty.kind() {
            ValueKind::List(ty) => {
                let array = item.concrete::<ListArray>()?;
                let values = array.values().as_ref();
                let remaining = member_range(array.value_offsets(), item.row, values.len())?;
                (
                    false,
                    Frame::Array {
                        values,
                        ty,
                        remaining,
                    },
                )
            }
            ValueKind::Struct(fields) => {
                let array = item.concrete::<StructArray>()?;
                if fields.len() != array.num_columns() {
                    return Err(invalid(
                        "JSON CAST row declaration differs from its Arrow children",
                    )
                    .into());
                }
                (
                    true,
                    Frame::Row {
                        array,
                        fields,
                        row: item.row,
                        next: 0,
                    },
                )
            }
            ValueKind::Map { key, value } => {
                let map = item.concrete::<MapArray>()?;
                let range = member_range(map.value_offsets(), item.row, map.entries().len())?;
                let (keys, _key_reservation) = map_members(map.keys().as_ref(), key, range, work)?;
                (
                    true,
                    Frame::Map {
                        values: map.values().as_ref(),
                        ty: value,
                        keys,
                        next: 0,
                        _key_reservation,
                    },
                )
            }
            ValueKind::Nominal(ValueLogicalType::Json) => {
                let node = Node::from_root(item.concrete::<LargeBinaryArray>()?.value(item.row))?;
                return Ok(Event::Item(Source::Encoded(node)));
            }
            _ => {
                let Some(value) = item.scalar()? else {
                    return Ok(Event::Item(Source::Null));
                };
                return Ok(Event::Item(match value {
                    Scalar::Date(_) | Scalar::Temporal { .. } => {
                        Source::InlineString(inline_text(value)?)
                    }
                    _ => Source::Scalar(value),
                }));
            }
        };
        if self.frames.len() == self.frames.capacity() {
            return Err(invalid("JSON CAST source depth exceeds its frozen type").into());
        }
        self.frames.push(frame);
        Ok(Event::Start(object))
    }
}

impl<'a, 'w, 'c> Scan<'a, RowWork<'w, 'c>> for SourceScan<'a> {
    fn advance(&mut self, work: &mut RowWork<'w, 'c>) -> Result<Option<Event<'a>>, JsonError> {
        work.step()?;
        if let Some(item) = self.next.take() {
            return self.item(item, work).map(Some);
        }
        let Some(frame) = self.frames.last_mut() else {
            return Ok(None);
        };
        match frame {
            Frame::Array {
                values,
                ty,
                remaining,
            } => {
                if let Some(row) = remaining.next() {
                    let item = Cell {
                        array: *values,
                        ty: *ty,
                        row,
                    };
                    return self.item(item, work).map(Some);
                }
            }
            Frame::Row {
                array,
                fields,
                row,
                next,
            } => {
                if let Some(field) = fields.get(*next) {
                    self.next = Some(Cell {
                        array: array.column(*next).as_ref(),
                        ty: field.value_type(),
                        row: *row,
                    });
                    *next += 1;
                    return Ok(Some(Event::Key(Source::Scalar(Scalar::String(
                        field.row_name().unwrap_or(""),
                    )))));
                }
            }
            Frame::Map {
                values,
                ty,
                keys,
                next,
                ..
            } => {
                while let Some(member) = keys.get(*next) {
                    *next += 1;
                    if let Some(later) = keys.get(*next)
                        && compare_keys(member.key.as_str(), later.key.as_str(), work)?.is_eq()
                    {
                        continue;
                    }
                    self.next = Some(Cell {
                        array: *values,
                        ty,
                        row: member.row,
                    });
                    let key = match &member.key {
                        MapKey::Borrowed(text) => Source::Scalar(Scalar::String(text)),
                        MapKey::Inline(text) => Source::InlineString(text.clone()),
                    };
                    return Ok(Some(Event::Key(key)));
                }
            }
        }
        self.frames.pop();
        Ok(Some(Event::End))
    }
}

fn checked_extent<T>(count: usize) -> Result<usize, KernelFailure> {
    count
        .checked_mul(size_of::<T>())
        .ok_or(KernelFailure::ResourceExhausted)
}

fn member_range(offsets: &[i32], row: usize, size: usize) -> Result<Range<usize>, KernelFailure> {
    let failure = || invalid("JSON CAST selected parent has invalid Arrow member offsets");
    let start = usize::try_from(*offsets.get(row).ok_or_else(failure)?).map_err(|_| failure())?;
    let end = usize::try_from(*offsets.get(row + 1).ok_or_else(failure)?).map_err(|_| failure())?;
    if start > end || end > size {
        Err(failure())
    } else {
        Ok(start..end)
    }
}

fn inline_text(value: Scalar<'_>) -> Result<FixedText<96>, KernelFailure> {
    let mut text = FixedText::new();
    write_scalar_text(&mut text, value)
        .map_err(|_| invalid("JSON CAST lost its bounded scalar text"))?;
    Ok(text)
}

fn map_members<'a>(
    keys: &'a dyn Array,
    ty: &'a ValueTypeRef,
    range: Range<usize>,
    work: &mut RowWork<'_, '_>,
) -> Result<(Vec<MapMember<'a>>, OpaqueReservation), KernelFailure> {
    let grant = work.reserve(checked_extent::<MapMember<'_>>(range.len())?)?;
    let mut members = Vec::new();
    members
        .try_reserve_exact(range.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    for row in range {
        work.step()?;
        let value = Cell {
            array: keys,
            ty,
            row,
        }
        .scalar()?
        .ok_or_else(|| invalid("JSON CAST saw an SQL NULL map key"))?;
        let key = match value {
            Scalar::String(text) => MapKey::Borrowed(text),
            _ => MapKey::Inline(inline_text(value)?),
        };
        members.push(MapMember { key, row });
    }
    sort_members(&mut members, work)?;
    Ok((members, grant))
}

fn compare_keys(
    left: &str,
    right: &str,
    work: &mut impl JsonWork,
) -> Result<Ordering, KernelFailure> {
    let mut left = left.encode_utf16();
    let mut right = right.encode_utf16();
    loop {
        work.step()?;
        match (left.next(), right.next()) {
            (Some(a), Some(b)) if a == b => {}
            (a, b) => return Ok(a.cmp(&b)),
        }
    }
}

/// Trino uses a string-keyed TreeMap: UTF-16 order, then the latest source
/// member wins if two original keys stringify to the same text.
fn sort_members(
    members: &mut [MapMember<'_>],
    work: &mut impl JsonWork,
) -> Result<(), KernelFailure> {
    fn sift(
        members: &mut [MapMember<'_>],
        mut top: usize,
        len: usize,
        work: &mut impl JsonWork,
    ) -> Result<(), KernelFailure> {
        while top < len / 2 {
            let left = top * 2 + 1;
            let right = left + 1;
            let compare = |a: usize, b: usize, work: &mut _| {
                Ok::<_, KernelFailure>(
                    compare_keys(members[a].key.as_str(), members[b].key.as_str(), work)?
                        .then_with(|| members[a].row.cmp(&members[b].row)),
                )
            };
            let large = if right < len && compare(left, right, work)?.is_lt() {
                right
            } else {
                left
            };
            if !compare(top, large, work)?.is_lt() {
                break;
            }
            members.swap(top, large);
            top = large;
        }
        Ok(())
    }
    for start in (0..members.len() / 2).rev() {
        sift(members, start, members.len(), work)?;
    }
    for tail in (1..members.len()).rev() {
        members.swap(0, tail);
        sift(members, 0, tail, work)?;
    }
    Ok(())
}
