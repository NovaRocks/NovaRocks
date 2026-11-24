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

//! Canonical protobuf structural preflight before generated DTO conversion.
//!
//! Durable MV facts reject unknown fields, duplicate singular fields and
//! noncanonical encodings. Recursion follows a closed, acyclic schema graph.

// Design: ADR-0224 (docs/adr/ADR-0224-mv-document-capacity-comes-from-real-boundaries.md)

use std::collections::{BTreeMap, BTreeSet};

use crate::persistence::codec::PersistenceCodecError;

#[derive(Clone, Copy, Debug)]
pub(super) enum Schema {
    DefinitionDocument,
    QuerySource,
    ResolutionContext,
    SourceFieldReference,
    InterpretationDocument,
    OutputBinding,
    StateSlot,
    ApplyKey,
    ApplyKeyComponent,
    AggregateInterpretation,
    PersistentAggregateContract,
    BranchInterpretation,
    TargetBinding,
    TargetPartitionFieldBinding,
    PhysicalFieldBinding,
    ConfigurationDocument,
    BindingDocument,
    DataVersionFormat,
    BindingOccurrence,
    BoundSourcePath,
    BoundSourceNode,
    BoundOutput,
    BindingDependency,
    ViewDependency,
    NestedMvDependency,
    BindingDigests,
    StorageMappingDocument,
    StorageColumnMapping,
    MarkerDocument,
    PendingMarker,
    SourceVersionWindow,
    PendingReleasedMarker,
    NeedsRebuildMarker,
    BindingChange,
    InvalidDefinitionMarker,
    InvalidMarker,
    RetirementRecord,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WireType {
    Varint,
    LengthDelimited,
}

#[derive(Clone, Copy, Debug)]
enum ScalarKind {
    Unsigned,
    Boolean,
}

#[derive(Clone, Copy, Debug)]
enum CanonicalKey {
    Bytes(u32),
    UnsignedThenBytes(u32, u32),
}

#[derive(Clone, Copy, Debug)]
struct Field {
    number: u32,
    wire_type: WireType,
    repeated: bool,
    packed_varints: bool,
    nested: Option<Schema>,
    scalar_kind: Option<ScalarKind>,
    canonical_key: Option<CanonicalKey>,
}

const fn scalar(number: u32) -> Field {
    Field {
        number,
        wire_type: WireType::Varint,
        repeated: false,
        packed_varints: false,
        nested: None,
        scalar_kind: Some(ScalarKind::Unsigned),
        canonical_key: None,
    }
}

const fn boolean(number: u32) -> Field {
    Field {
        number,
        wire_type: WireType::Varint,
        repeated: false,
        packed_varints: false,
        nested: None,
        scalar_kind: Some(ScalarKind::Boolean),
        canonical_key: None,
    }
}

const fn bytes(number: u32) -> Field {
    Field {
        number,
        wire_type: WireType::LengthDelimited,
        repeated: false,
        packed_varints: false,
        nested: None,
        scalar_kind: None,
        canonical_key: None,
    }
}

const fn repeated_bytes(number: u32) -> Field {
    Field {
        number,
        wire_type: WireType::LengthDelimited,
        repeated: true,
        packed_varints: false,
        nested: None,
        scalar_kind: None,
        canonical_key: None,
    }
}

const fn packed_varints(number: u32) -> Field {
    Field {
        number,
        wire_type: WireType::LengthDelimited,
        repeated: true,
        packed_varints: true,
        nested: None,
        scalar_kind: None,
        canonical_key: None,
    }
}

const fn message(number: u32, nested: Schema) -> Field {
    Field {
        number,
        wire_type: WireType::LengthDelimited,
        repeated: false,
        packed_varints: false,
        nested: Some(nested),
        scalar_kind: None,
        canonical_key: None,
    }
}

const fn repeated_message(number: u32, nested: Schema) -> Field {
    Field {
        number,
        wire_type: WireType::LengthDelimited,
        repeated: true,
        packed_varints: false,
        nested: Some(nested),
        scalar_kind: None,
        canonical_key: None,
    }
}

const fn repeated_set_message(number: u32, nested: Schema, key: CanonicalKey) -> Field {
    Field {
        number,
        wire_type: WireType::LengthDelimited,
        repeated: true,
        packed_varints: false,
        nested: Some(nested),
        scalar_kind: None,
        canonical_key: Some(key),
    }
}

const DEFINITION: &[Field] = &[scalar(1), message(2, Schema::QuerySource), scalar(6)];
const QUERY: &[Field] = &[bytes(1), scalar(2), message(3, Schema::ResolutionContext)];
const RESOLUTION: &[Field] = &[bytes(1), bytes(2)];
const SOURCE_FIELD_REFERENCE: &[Field] = &[scalar(1), bytes(2)];
const INTERPRETATION: &[Field] = &[
    scalar(1),
    bytes(2),
    repeated_set_message(4, Schema::OutputBinding, CanonicalKey::Bytes(1)),
    repeated_set_message(5, Schema::StateSlot, CanonicalKey::Bytes(1)),
    message(6, Schema::ApplyKey),
    repeated_set_message(7, Schema::AggregateInterpretation, CanonicalKey::Bytes(1)),
    repeated_message(8, Schema::BranchInterpretation),
    message(9, Schema::TargetBinding),
];
const OUTPUT_BINDING: &[Field] = &[bytes(1), bytes(2), bytes(3), boolean(4)];
const STATE_SLOT: &[Field] = &[
    bytes(1),
    bytes(2),
    bytes(3),
    boolean(4),
    scalar(5),
    scalar(6),
];
const APPLY_KEY: &[Field] = &[scalar(1), repeated_message(2, Schema::ApplyKeyComponent)];
const APPLY_KEY_COMPONENT: &[Field] = &[bytes(1), bytes(2)];
const AGGREGATE: &[Field] = &[
    bytes(1),
    bytes(2),
    repeated_set_message(
        3,
        Schema::SourceFieldReference,
        CanonicalKey::UnsignedThenBytes(1, 2),
    ),
    repeated_bytes(4),
    bytes(5),
    message(6, Schema::PersistentAggregateContract),
    bytes(7),
];
// Repeated uint32 is encoded packed and therefore length-delimited.
const PERSISTENT_AGGREGATE: &[Field] = &[bytes(1), bytes(2), bytes(4)];
const BRANCH: &[Field] = &[bytes(1), packed_varints(2), repeated_bytes(3)];
const TARGET: &[Field] = &[
    bytes(1),
    bytes(2),
    bytes(3),
    repeated_set_message(
        4,
        Schema::PhysicalFieldBinding,
        CanonicalKey::UnsignedThenBytes(1, 2),
    ),
    repeated_message(5, Schema::TargetPartitionFieldBinding),
];
const TARGET_PARTITION_FIELD: &[Field] = &[bytes(1), bytes(2), scalar(3), scalar(4)];
const PHYSICAL_FIELD: &[Field] = &[scalar(1), bytes(2), bytes(3), bytes(4), boolean(5)];
const CONFIGURATION: &[Field] = &[scalar(1), scalar(2), boolean(3), scalar(4), scalar(5)];

const BINDING: &[Field] = &[
    scalar(1),
    repeated_message(3, Schema::BindingOccurrence),
    repeated_message(4, Schema::BoundOutput),
    repeated_message(5, Schema::BindingDependency),
    message(6, Schema::BindingDigests),
];
const BINDING_OCCURRENCE: &[Field] = &[
    scalar(1),
    bytes(2),
    bytes(3),
    bytes(4),
    bytes(5),
    bytes(6),
    scalar(7),
    message(8, Schema::DataVersionFormat),
    bytes(9),
    bytes(10),
    bytes(11),
    bytes(12),
    repeated_message(13, Schema::BoundSourcePath),
    bytes(14),
];
const BOUND_SOURCE_PATH: &[Field] = &[
    repeated_message(1, Schema::BoundSourceNode),
    bytes(2),
    boolean(3),
];
const BOUND_SOURCE_NODE: &[Field] = &[bytes(1), boolean(2), scalar(3)];
const BOUND_OUTPUT: &[Field] = &[
    scalar(1),
    bytes(2),
    bytes(3),
    repeated_set_message(
        4,
        Schema::SourceFieldReference,
        CanonicalKey::UnsignedThenBytes(1, 2),
    ),
];
const BINDING_DEPENDENCY: &[Field] = &[
    message(1, Schema::ViewDependency),
    message(2, Schema::NestedMvDependency),
    bytes(3),
];
const VIEW_DEPENDENCY: &[Field] = &[
    bytes(1),
    scalar(2),
    bytes(3),
    bytes(4),
    scalar(5),
    scalar(6),
];
const NESTED_MV_DEPENDENCY: &[Field] = &[bytes(1), bytes(2), scalar(3)];
const BINDING_DIGESTS: &[Field] = &[bytes(1), bytes(2), bytes(3)];
const STORAGE_MAPPING: &[Field] = &[scalar(1), repeated_message(2, Schema::StorageColumnMapping)];
const STORAGE_COLUMN: &[Field] = &[
    scalar(1),
    bytes(2),
    bytes(3),
    bytes(4),
    bytes(5),
    bytes(6),
    bytes(7),
    scalar(8),
    boolean(9),
    bytes(10),
    scalar(11),
];
const MARKER: &[Field] = &[
    scalar(1),
    bytes(2),
    bytes(3),
    message(4, Schema::PendingMarker),
    message(5, Schema::PendingReleasedMarker),
    message(6, Schema::NeedsRebuildMarker),
    message(7, Schema::InvalidDefinitionMarker),
    message(8, Schema::InvalidMarker),
];
const PENDING_MARKER: &[Field] = &[
    bytes(1),
    bytes(2),
    repeated_message(3, Schema::SourceVersionWindow),
    bytes(4),
    scalar(5),
    bytes(6),
    bytes(7),
];
const SOURCE_VERSION_WINDOW: &[Field] = &[scalar(1), bytes(2), bytes(3)];
const PENDING_RELEASED: &[Field] = &[bytes(1)];
const NEEDS_REBUILD: &[Field] = &[repeated_message(1, Schema::BindingChange)];
const BINDING_CHANGE: &[Field] = &[scalar(1), bytes(2), bytes(3)];
const INVALID_DEFINITION: &[Field] = &[scalar(1), bytes(2)];
const INVALID_MARKER: &[Field] = &[bytes(1)];
const RETIREMENT_RECORD: &[Field] = &[
    scalar(1),
    bytes(2),
    bytes(3),
    scalar(4),
    scalar(5),
    scalar(6),
    bytes(7),
];

const DATA_VERSION_FORMAT: &[Field] = &[bytes(1), bytes(2), scalar(3), scalar(4)];

impl Schema {
    fn fields(self) -> &'static [Field] {
        match self {
            Self::DefinitionDocument => DEFINITION,
            Self::QuerySource => QUERY,
            Self::ResolutionContext => RESOLUTION,
            Self::SourceFieldReference => SOURCE_FIELD_REFERENCE,
            Self::InterpretationDocument => INTERPRETATION,
            Self::OutputBinding => OUTPUT_BINDING,
            Self::StateSlot => STATE_SLOT,
            Self::ApplyKey => APPLY_KEY,
            Self::ApplyKeyComponent => APPLY_KEY_COMPONENT,
            Self::AggregateInterpretation => AGGREGATE,
            Self::PersistentAggregateContract => PERSISTENT_AGGREGATE,
            Self::BranchInterpretation => BRANCH,
            Self::TargetBinding => TARGET,
            Self::TargetPartitionFieldBinding => TARGET_PARTITION_FIELD,
            Self::PhysicalFieldBinding => PHYSICAL_FIELD,
            Self::ConfigurationDocument => CONFIGURATION,
            Self::BindingDocument => BINDING,
            Self::DataVersionFormat => DATA_VERSION_FORMAT,
            Self::BindingOccurrence => BINDING_OCCURRENCE,
            Self::BoundSourcePath => BOUND_SOURCE_PATH,
            Self::BoundSourceNode => BOUND_SOURCE_NODE,
            Self::BoundOutput => BOUND_OUTPUT,
            Self::BindingDependency => BINDING_DEPENDENCY,
            Self::ViewDependency => VIEW_DEPENDENCY,
            Self::NestedMvDependency => NESTED_MV_DEPENDENCY,
            Self::BindingDigests => BINDING_DIGESTS,
            Self::StorageMappingDocument => STORAGE_MAPPING,
            Self::StorageColumnMapping => STORAGE_COLUMN,
            Self::MarkerDocument => MARKER,
            Self::PendingMarker => PENDING_MARKER,
            Self::SourceVersionWindow => SOURCE_VERSION_WINDOW,
            Self::PendingReleasedMarker => PENDING_RELEASED,
            Self::NeedsRebuildMarker => NEEDS_REBUILD,
            Self::BindingChange => BINDING_CHANGE,
            Self::InvalidDefinitionMarker => INVALID_DEFINITION,
            Self::InvalidMarker => INVALID_MARKER,
            Self::RetirementRecord => RETIREMENT_RECORD,
        }
    }
}

pub(super) fn preflight(bytes: &[u8], schema: Schema) -> Result<(), PersistenceCodecError> {
    scan_message(bytes, schema)
}

fn scan_message(mut bytes: &[u8], schema: Schema) -> Result<(), PersistenceCodecError> {
    let mut seen_singular = BTreeSet::new();
    let mut previous_field_number = 0;
    let mut previous_set_keys = BTreeMap::<u32, Vec<u8>>::new();
    while !bytes.is_empty() {
        let (key, key_len) = read_varint(bytes)?;
        bytes = &bytes[key_len..];
        let number =
            u32::try_from(key >> 3).map_err(|_| malformed("field number does not fit u32"))?;
        if number == 0 {
            return Err(malformed("field number zero is invalid"));
        }
        if number < previous_field_number {
            return Err(malformed(format!(
                "field {number} in {schema:?} is not in canonical tag order"
            )));
        }
        previous_field_number = number;
        let actual_wire = match key & 0x07 {
            0 => WireType::Varint,
            2 => WireType::LengthDelimited,
            value => return Err(malformed(format!("unsupported wire type {value}"))),
        };
        let Some(field) = schema.fields().iter().find(|field| field.number == number) else {
            return Err(malformed(format!("unknown field {number} in {schema:?}")));
        };
        if actual_wire != field.wire_type {
            return Err(malformed(format!(
                "field {number} in {schema:?} uses the wrong wire type"
            )));
        }
        if (!field.repeated || field.packed_varints) && !seen_singular.insert(number) {
            return Err(malformed(format!(
                "singular field {number} occurs more than once in {schema:?}"
            )));
        }

        match actual_wire {
            WireType::Varint => {
                let (value, consumed) = read_varint(bytes)?;
                if matches!(field.scalar_kind, Some(ScalarKind::Boolean)) && value > 1 {
                    return Err(malformed(format!(
                        "boolean field {number} in {schema:?} has illegal value {value}"
                    )));
                }
                bytes = &bytes[consumed..];
            }
            WireType::LengthDelimited => {
                let (length, prefix_len) = read_varint(bytes)?;
                bytes = &bytes[prefix_len..];
                let length = usize::try_from(length)
                    .map_err(|_| malformed("length-delimited field is too large"))?;
                if bytes.len() < length {
                    return Err(malformed("truncated length-delimited field"));
                }
                let value = &bytes[..length];
                bytes = &bytes[length..];
                if field.packed_varints {
                    let mut packed = value;
                    while !packed.is_empty() {
                        let (_, consumed) = read_varint(packed)?;
                        packed = &packed[consumed..];
                    }
                } else if let Some(nested) = field.nested {
                    scan_message(value, nested)?;
                    if let Some(key_spec) = field.canonical_key {
                        let key = extract_canonical_key(value, key_spec)?;
                        if let Some(previous) = previous_set_keys.get(&number)
                            && previous >= &key
                        {
                            return Err(malformed(format!(
                                "repeated field {number} in {schema:?} has duplicate or noncanonical set order"
                            )));
                        }
                        previous_set_keys.insert(number, key);
                    }
                }
            }
        }
    }
    Ok(())
}

fn read_varint(bytes: &[u8]) -> Result<(u64, usize), PersistenceCodecError> {
    let mut value = 0_u64;
    for (index, byte) in bytes.iter().copied().take(10).enumerate() {
        let bits = u64::from(byte & 0x7f);
        if index == 9 && bits > 1 {
            return Err(malformed("varint overflow"));
        }
        value |= bits << (index * 7);
        if byte & 0x80 == 0 {
            if index > 0 && bits == 0 {
                return Err(malformed("non-minimal varint encoding"));
            }
            return Ok((value, index + 1));
        }
    }
    Err(malformed("truncated or overlong varint"))
}

fn extract_canonical_key(
    bytes: &[u8],
    key: CanonicalKey,
) -> Result<Vec<u8>, PersistenceCodecError> {
    match key {
        CanonicalKey::Bytes(number) => read_key_bytes(bytes, number),
        CanonicalKey::UnsignedThenBytes(unsigned_field, bytes_field) => {
            let (unsigned, remaining) = read_key_unsigned(bytes, unsigned_field)?;
            let mut key = unsigned.to_be_bytes().to_vec();
            key.extend_from_slice(&read_key_bytes(remaining, bytes_field)?);
            Ok(key)
        }
    }
}

fn read_key_unsigned(
    bytes: &[u8],
    expected_field: u32,
) -> Result<(u64, &[u8]), PersistenceCodecError> {
    let (tag, tag_len) = read_varint(bytes)?;
    if tag != u64::from(expected_field) << 3 {
        return Err(malformed("canonical set key scalar field is missing"));
    }
    let (value, value_len) = read_varint(&bytes[tag_len..])?;
    Ok((value, &bytes[tag_len + value_len..]))
}

fn read_key_bytes(bytes: &[u8], expected_field: u32) -> Result<Vec<u8>, PersistenceCodecError> {
    let (tag, tag_len) = read_varint(bytes)?;
    if tag != (u64::from(expected_field) << 3) | 2 {
        return Err(malformed("canonical set key bytes field is missing"));
    }
    let (length, length_len) = read_varint(&bytes[tag_len..])?;
    let length =
        usize::try_from(length).map_err(|_| malformed("canonical set key is too large"))?;
    let start = tag_len + length_len;
    let end = start
        .checked_add(length)
        .ok_or_else(|| malformed("canonical set key length overflow"))?;
    let value = bytes
        .get(start..end)
        .ok_or_else(|| malformed("truncated canonical set key"))?;
    Ok(value.to_vec())
}

fn malformed(message: impl Into<String>) -> PersistenceCodecError {
    PersistenceCodecError::MalformedWire(message.into())
}
