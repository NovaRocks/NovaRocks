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

use super::*;
use std::fmt::{self, Write};
use std::mem::size_of;

const MAX_FIELDS: usize = 4096;
const MAX_NAME: usize = 64 * 1024;
// InternalFacts single-fact-set metadata. This includes names, types, metadata
// and actual field/token Vec capacities, not just serialized logical bytes.
const MAX_METADATA: usize = 16 * 1024 * 1024;
const MAX_DEPTH: usize = 64;
// Engineering traversal bound including every repeated type/field reference
// and metadata entry. It precedes recursive Arrow Debug formatting.
const MAX_NODES: usize = 16 * 1024;
const MAX_DEBUG: usize = MAX_CONNECTOR_ROW_CONVERSION_WORKSPACE_BYTES;

fn exhausted() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "row-mutation match contract exceeds its bounded metadata or traversal",
    )
}
#[derive(Default)]
struct Bound {
    bytes: usize,
    nodes: usize,
}
impl Bound {
    fn charge(&mut self, bytes: usize) -> Result<(), ConnectorError> {
        self.bytes = self.bytes.checked_add(bytes).ok_or_else(exhausted)?;
        if self.bytes > MAX_METADATA {
            return Err(exhausted());
        }
        Ok(())
    }
    fn slots<T>(&mut self, capacity: usize) -> Result<(), ConnectorError> {
        self.charge(capacity.checked_mul(size_of::<T>()).ok_or_else(exhausted)?)
    }
    fn visit(&mut self, depth: usize) -> Result<(), ConnectorError> {
        if depth >= MAX_DEPTH || self.nodes >= MAX_NODES {
            return Err(exhausted());
        }
        self.nodes += 1;
        Ok(())
    }
    fn metadata(&mut self, metadata: &arrow::datatypes::Metadata) -> Result<(), ConnectorError> {
        // Arrow 60 metadata retains one shared Arc and a BTree with no
        // deleted buckets. Its source-derived node bound precedes traversal.
        self.charge(
            novarocks_type_contract::owned_resources::arrow_metadata::retained_backing_upper_bound(
                metadata.len(),
            )
            .map_err(|_| exhausted())?,
        )?;
        for (key, value) in metadata {
            self.visit(0)?;
            self.charge(key.capacity())?;
            self.charge(value.capacity())?;
        }
        Ok(())
    }
    fn field(&mut self, field: &Field, depth: usize) -> Result<(), ConnectorError> {
        self.visit(depth)?;
        if field.name().len() > MAX_NAME {
            return Err(exhausted());
        }
        self.charge(size_of::<Field>() + 2 * size_of::<usize>())?;
        self.charge(field.name().capacity())?;
        self.metadata(field.metadata())?;
        self.data_type(field.data_type(), depth)
    }
    fn data_type(&mut self, dt: &DataType, depth: usize) -> Result<(), ConnectorError> {
        self.visit(depth)?;
        self.charge(size_of::<DataType>())?;
        match dt {
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::ListView(f)
            | DataType::LargeListView(f)
            | DataType::FixedSizeList(f, _)
            | DataType::Map(f, _) => self.field(f, depth + 1)?,
            DataType::Struct(fields) => {
                self.charge(2 * size_of::<usize>())?;
                self.slots::<std::sync::Arc<Field>>(fields.len())?;
                for f in fields {
                    self.field(f, depth + 1)?;
                }
            }
            DataType::Union(fields, _) => {
                self.charge(2 * size_of::<usize>())?;
                self.slots::<(i8, std::sync::Arc<Field>)>(fields.len())?;
                for (_, f) in fields.iter() {
                    self.field(f, depth + 1)?;
                }
            }
            DataType::Dictionary(k, v) => {
                self.data_type(k, depth + 1)?;
                self.data_type(v, depth + 1)?;
            }
            DataType::RunEndEncoded(r, v) => {
                self.field(r, depth + 1)?;
                self.field(v, depth + 1)?;
            }
            DataType::Timestamp(_, Some(zone)) => {
                self.charge(zone.len())?;
                self.charge(2 * size_of::<usize>())?;
            }
            _ => {}
        }
        Ok(())
    }
}

// Vec capacity is part of the retained source proof.
#[allow(clippy::ptr_arg)]
pub(super) fn preflight(
    identity: &Vec<ConnectorMutationSourceField>,
    before: &Vec<ConnectorMutationTargetField>,
    after: &Vec<ConnectorMutationTargetField>,
    unique: &Vec<ConnectorWriteFieldToken>,
    effect: &ConnectorMutationEffectField,
) -> Result<(), ConnectorError> {
    let count = identity
        .len()
        .checked_add(before.len())
        .and_then(|n| n.checked_add(after.len()))
        .and_then(|n| n.checked_add(1))
        .ok_or_else(exhausted)?;
    if count > MAX_FIELDS || unique.len() > MAX_FIELDS {
        return Err(exhausted());
    }
    let mut bound = Bound::default();
    bound.slots::<ConnectorMutationSourceField>(identity.capacity())?;
    bound.slots::<ConnectorMutationTargetField>(before.capacity())?;
    bound.slots::<ConnectorMutationTargetField>(after.capacity())?;
    bound.slots::<ConnectorWriteFieldToken>(unique.capacity())?;
    for f in identity {
        bound.field(&f.field, 0)?;
    }
    for f in before.iter().chain(after) {
        bound.field(&f.field, 0)?;
    }
    bound.field(&effect.field, 0)?;
    let mut debug = DebugCount::default();
    for f in identity {
        write!(debug, "{:?}", f.field).map_err(|_| exhausted())?;
    }
    for f in before.iter().chain(after) {
        write!(debug, "{:?}", f.field).map_err(|_| exhausted())?;
    }
    write!(debug, "{:?}", effect.field).map_err(|_| exhausted())?;
    Ok(())
}

#[derive(Default)]
struct DebugCount {
    bytes: usize,
}
impl Write for DebugCount {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let next = self.bytes.checked_add(value.len()).ok_or(fmt::Error)?;
        if next > MAX_DEBUG {
            return Err(fmt::Error);
        }
        self.bytes = next;
        Ok(())
    }
}
struct DebugHash<'a> {
    hasher: &'a mut Sha256,
    remaining: usize,
}
impl Write for DebugHash<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.remaining = self.remaining.checked_sub(value.len()).ok_or(fmt::Error)?;
        self.hasher.update(value.as_bytes());
        Ok(())
    }
}

pub(super) fn digest_debug_field(hasher: &mut Sha256, field: &Field) -> Result<(), ConnectorError> {
    // The v1 seal includes a byte-length prefix followed by Arrow's exact Debug
    // representation. Borrowed count + streaming hash preserves metadata's old
    // iteration order and avoids a full formatted String or field copy.
    let mut count = DebugCount::default();
    write!(count, "{field:?}").map_err(|_| exhausted())?;
    hasher.update((count.bytes as u64).to_be_bytes());
    let mut output = DebugHash {
        hasher,
        remaining: count.bytes,
    };
    write!(output, "{field:?}").map_err(|_| exhausted())?;
    if output.remaining != 0 {
        return Err(exhausted());
    }
    Ok(())
}

/// Independent full value identity is part of the signed match contract.
/// Existing source Field/attrs hashing stays separate and supplies no meaning.
/// Reuse the one shared, borrowed type/layout emitter; no formatted shadow
/// tree, allocation, runtime encoding fingerprint or new receiving limit.
pub(super) fn digest_execution_type(
    hasher: &mut Sha256,
    value: &novarocks_type_contract::FunctionValueType,
) -> Result<(), ConnectorError> {
    let mut bytes = 0usize;
    let mut write = |part: &[u8]| {
        bytes = bytes.checked_add(part.len()).ok_or_else(exhausted)?;
        // This stream allocates no formatted workspace. The original source
        // preflight still bounds Field/name/type shape and Debug projection;
        // do not impose a second receiving threshold on shared type identity.
        hasher.update(part);
        Ok::<(), ConnectorError>(())
    };
    novarocks_type_contract::write_value_type_identity_observed(value.value_type(), &mut write)?;
    novarocks_type_contract::write_value_layout_identity_observed(
        value.typed_binding().layout(),
        &mut write,
    )
}
