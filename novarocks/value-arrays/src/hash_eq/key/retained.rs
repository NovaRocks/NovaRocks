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

//! Typed access to an already owned representative. This lends the original
//! state value to the same relations used by Arrow-backed batch readers.
use super::*;
use novarocks_type_contract::TemporalValue;

#[derive(Clone, Copy)]
pub enum RetainedKeyChild {
    Item,
    Field,
    MapKey,
    MapValue,
}

pub enum RetainedKeyContents<'a> {
    Boolean(bool),
    Signed(i64),
    Float32(f32),
    Float64(f64),
    Decimal128(i128),
    Decimal256([u8; 32]),
    Bytes(&'a [u8]),
    Fixed16([u8; 16]),
    Temporal(TemporalValue),
    List(usize),
    Struct(usize),
    Map(usize),
}

/// A selected, already admitted state value. The original state owner retains
/// its backing. The caller supplies the frozen type independently of storage.
pub trait RetainedKeyValue {
    fn contents(&self, leaf: Option<KeyLeaf>) -> Result<RetainedKeyContents<'_>, ValueError>;

    /// The relation requests only children of a reachable non-NULL value and
    /// coordinates within its actual reported extent.
    fn child(&self, kind: RetainedKeyChild, index: usize) -> Option<&dyn RetainedKeyValue>;
}

pub(super) fn read<'a, E>(
    position: Position<'a>,
    value: &'a dyn RetainedKeyValue,
    work: &mut Work<'_, E>,
) -> Result<Value<'a>, ValueFailure<E>> {
    use RetainedKeyContents as Contents;
    let leaf = key_leaf(position.ty).map_err(|_| invalid(ValueViolation::Carrier))?;
    Ok(match (leaf, value.contents(leaf)?) {
        (Some(KeyLeaf::Boolean), Contents::Boolean(value)) => Value::fixed([u8::from(value)]),
        (Some(KeyLeaf::Signed), Contents::Signed(value)) => Value::fixed(value.to_le_bytes()),
        (Some(KeyLeaf::Float32), Contents::Float32(value)) => Value::Float32(value),
        (Some(KeyLeaf::Float64), Contents::Float64(value)) => Value::Float64(value),
        (Some(KeyLeaf::Decimal128), Contents::Decimal128(value)) => {
            Value::fixed(value.to_le_bytes())
        }
        (Some(KeyLeaf::Decimal256), Contents::Decimal256(bytes)) => Value::fixed(bytes),
        (Some(KeyLeaf::Bytes | KeyLeaf::FixedBytes), Contents::Bytes(bytes)) => Value::Bytes(bytes),
        (Some(KeyLeaf::Character(character)), Contents::Bytes(bytes)) => Value::Bytes(
            key_character_bytes_observed(character, bytes, &mut *work)
                .map_err(ValueFailure::Caller)?,
        ),
        (Some(KeyLeaf::FixedBytes), Contents::Fixed16(bytes)) => Value::fixed(bytes),
        (Some(KeyLeaf::Temporal(declaration)), Contents::Temporal(value)) => {
            let key = value
                .relation_key(declaration)
                .map_err(|_| invalid(ValueViolation::Carrier))?;
            Value::fixed(key.order_bytes())
        }
        (None, Contents::List(len))
            if matches!(
                position.ty.kind(),
                ValueKind::List(_) | ValueKind::FixedSizeList { .. }
            ) =>
        {
            Value::Sequence(Sequence::Retained {
                position,
                value,
                len,
                kind: RetainedKeyChild::Item,
            })
        }
        (None, Contents::Struct(len)) if matches!(position.ty.kind(), ValueKind::Struct(fields) if fields.len() == len) => {
            Value::Sequence(Sequence::Retained {
                position,
                value,
                len,
                kind: RetainedKeyChild::Field,
            })
        }
        (None, Contents::Map(len)) if matches!(position.ty.kind(), ValueKind::Map { .. }) => {
            Value::Map(MapValue {
                position,
                source: MapSource::Retained(value),
                rows: 0..len,
            })
        }
        _ => return Err(invalid(ValueViolation::Carrier).into()),
    })
}
