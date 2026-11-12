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

use crate::iceberg::spec::{Literal, PrimitiveLiteral, Struct, TableMetadata, Type};

const MAX_TIME_MICROS: i64 = 24 * 60 * 60 * 1_000_000 - 1;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum IcebergWriteDescriptorError {
    MissingDescriptor,
    UnknownPartitionSpec { spec_id: i32 },
    FieldCountMismatch { expected: usize, actual: usize },
    MissingPayload { index: usize },
    DecodeFailed { index: usize, message: String },
}

impl IcebergWriteDescriptorError {
    pub fn code(&self) -> &'static str {
        "IcebergWriteDescriptorMismatch"
    }

    pub fn detail_message(&self) -> String {
        match self {
            Self::MissingDescriptor => "missing partition descriptor".to_string(),
            Self::UnknownPartitionSpec { spec_id } => {
                format!("unknown partition spec id {spec_id}")
            }
            Self::FieldCountMismatch { expected, actual } => {
                format!(
                    "partition descriptor field count mismatch: expected {expected}, got {actual}"
                )
            }
            Self::MissingPayload { index } => {
                format!("partition descriptor value {index} is non-null but has no payload")
            }
            Self::DecodeFailed { index, message } => {
                format!("decode partition descriptor value {index} failed: {message}")
            }
        }
    }
}

impl std::fmt::Display for IcebergWriteDescriptorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code(), self.detail_message())
    }
}

impl std::error::Error for IcebergWriteDescriptorError {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IcebergPartitionDescriptor {
    pub values: Vec<IcebergPartitionValueDescriptor>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IcebergPartitionValueDescriptor {
    pub is_null: bool,
    pub datum_bytes: Option<Vec<u8>>,
}

pub fn encode_partition_descriptor(
    values: &Struct,
    partition_spec_id: i32,
    metadata: &TableMetadata,
) -> Result<IcebergPartitionDescriptor, IcebergWriteDescriptorError> {
    let spec = metadata.partition_spec_by_id(partition_spec_id).ok_or(
        IcebergWriteDescriptorError::UnknownPartitionSpec {
            spec_id: partition_spec_id,
        },
    )?;
    let partition_type = spec
        .partition_type(metadata.current_schema().as_ref())
        .map_err(|e| IcebergWriteDescriptorError::DecodeFailed {
            index: 0,
            message: e.to_string(),
        })?;
    encode_partition_descriptor_with_type(values, &partition_type)
}

/// Encode the historical partition type authored by the pinned mutation endpoint.
pub(crate) fn encode_partition_descriptor_with_type(
    values: &Struct,
    partition_type: &crate::iceberg::spec::StructType,
) -> Result<IcebergPartitionDescriptor, IcebergWriteDescriptorError> {
    if values.fields().len() != partition_type.fields().len() {
        return Err(IcebergWriteDescriptorError::FieldCountMismatch {
            expected: partition_type.fields().len(),
            actual: values.fields().len(),
        });
    }

    let mut encoded = Vec::with_capacity(values.fields().len());
    for (idx, value) in values.fields().iter().enumerate() {
        match value {
            None => encoded.push(IcebergPartitionValueDescriptor {
                is_null: true,
                datum_bytes: None,
            }),
            Some(Literal::Primitive(primitive)) => {
                let field_type = partition_type.fields()[idx].field_type.as_ref();
                let crate::iceberg::spec::Type::Primitive(primitive_type) = field_type else {
                    return Err(IcebergWriteDescriptorError::DecodeFailed {
                        index: idx,
                        message: format!("partition field type is not primitive: {field_type:?}"),
                    });
                };
                encoded.push(IcebergPartitionValueDescriptor {
                    is_null: false,
                    datum_bytes: Some(
                        primitive_literal_to_iceberg_bytes(primitive, primitive_type).map_err(
                            |message| IcebergWriteDescriptorError::DecodeFailed {
                                index: idx,
                                message,
                            },
                        )?,
                    ),
                });
            }
            Some(other) => {
                return Err(IcebergWriteDescriptorError::DecodeFailed {
                    index: idx,
                    message: format!(
                        "partition descriptor only supports primitive literals, got {other:?}"
                    ),
                });
            }
        }
    }

    Ok(IcebergPartitionDescriptor { values: encoded })
}

pub fn decode_partition_descriptor(
    desc: Option<IcebergPartitionDescriptor>,
    partition_spec_id: i32,
    metadata: &TableMetadata,
) -> Result<Struct, IcebergWriteDescriptorError> {
    let desc = desc.ok_or(IcebergWriteDescriptorError::MissingDescriptor)?;
    let spec = metadata.partition_spec_by_id(partition_spec_id).ok_or(
        IcebergWriteDescriptorError::UnknownPartitionSpec {
            spec_id: partition_spec_id,
        },
    )?;
    let partition_type = spec
        .partition_type(metadata.current_schema().as_ref())
        .map_err(|e| IcebergWriteDescriptorError::DecodeFailed {
            index: 0,
            message: e.to_string(),
        })?;
    decode_partition_descriptor_with_type(&desc, &partition_type)
}

/// Decode an artifact against its actual historical write interpretation.
pub(crate) fn decode_partition_descriptor_with_type(
    desc: &IcebergPartitionDescriptor,
    partition_type: &crate::iceberg::spec::StructType,
) -> Result<Struct, IcebergWriteDescriptorError> {
    let values = &desc.values;
    if values.len() != partition_type.fields().len() {
        return Err(IcebergWriteDescriptorError::FieldCountMismatch {
            expected: partition_type.fields().len(),
            actual: values.len(),
        });
    }

    let mut decoded = Vec::with_capacity(values.len());
    for (idx, value) in values.iter().enumerate() {
        if value.is_null {
            if value.datum_bytes.is_some() {
                return Err(IcebergWriteDescriptorError::DecodeFailed {
                    index: idx,
                    message: "partition descriptor null value must not carry payload".to_string(),
                });
            }
            decoded.push(None);
            continue;
        }

        let bytes = value
            .datum_bytes
            .as_deref()
            .ok_or(IcebergWriteDescriptorError::MissingPayload { index: idx })?;
        let field_type = partition_type.fields()[idx].field_type.as_ref();
        let crate::iceberg::spec::Type::Primitive(primitive_type) = field_type else {
            return Err(IcebergWriteDescriptorError::DecodeFailed {
                index: idx,
                message: format!("partition field type is not primitive: {field_type:?}"),
            });
        };
        validate_descriptor_payload_bytes(bytes, primitive_type, idx)?;
        let datum = crate::iceberg::spec::Datum::try_from_bytes(bytes, primitive_type.clone())
            .map_err(|e| IcebergWriteDescriptorError::DecodeFailed {
                index: idx,
                message: e.to_string(),
            })?;
        let literal = datum.literal().clone();
        validate_decoded_primitive_literal(&literal, primitive_type, idx)?;
        decoded.push(Some(Literal::Primitive(literal)));
    }

    Ok(Struct::from_iter(decoded))
}

fn primitive_literal_to_iceberg_bytes(
    literal: &PrimitiveLiteral,
    primitive_type: &crate::iceberg::spec::PrimitiveType,
) -> Result<Vec<u8>, String> {
    if !primitive_type.compatible(literal) {
        return Err(format!(
            "partition literal {literal:?} is incompatible with type {primitive_type:?}"
        ));
    }

    let bytes = match (literal, primitive_type) {
        (PrimitiveLiteral::Boolean(val), crate::iceberg::spec::PrimitiveType::Boolean) => {
            vec![u8::from(*val)]
        }
        (
            PrimitiveLiteral::Int(val),
            crate::iceberg::spec::PrimitiveType::Int | crate::iceberg::spec::PrimitiveType::Date,
        ) => val.to_le_bytes().to_vec(),
        (PrimitiveLiteral::Long(val), crate::iceberg::spec::PrimitiveType::Time) => {
            if !time_micros_in_range(*val) {
                return Err(format!(
                    "partition time literal {val} is outside valid Iceberg time range 0..={MAX_TIME_MICROS}"
                ));
            }
            val.to_le_bytes().to_vec()
        }
        (
            PrimitiveLiteral::Long(val),
            crate::iceberg::spec::PrimitiveType::Long
            | crate::iceberg::spec::PrimitiveType::Timestamp
            | crate::iceberg::spec::PrimitiveType::Timestamptz
            | crate::iceberg::spec::PrimitiveType::TimestampNs
            | crate::iceberg::spec::PrimitiveType::TimestamptzNs,
        ) => val.to_le_bytes().to_vec(),
        (PrimitiveLiteral::Float(val), crate::iceberg::spec::PrimitiveType::Float) => {
            val.0.to_le_bytes().to_vec()
        }
        (PrimitiveLiteral::Double(val), crate::iceberg::spec::PrimitiveType::Double) => {
            val.0.to_le_bytes().to_vec()
        }
        (PrimitiveLiteral::String(val), crate::iceberg::spec::PrimitiveType::String) => {
            val.as_bytes().to_vec()
        }
        (PrimitiveLiteral::Binary(val), crate::iceberg::spec::PrimitiveType::Binary) => val.clone(),
        (PrimitiveLiteral::Binary(val), crate::iceberg::spec::PrimitiveType::Fixed(len)) => {
            let expected = usize::try_from(*len)
                .map_err(|_| format!("fixed length {len} cannot fit in usize"))?;
            if val.len() != expected {
                return Err(format!(
                    "partition binary literal length {} does not match fixed length {expected}",
                    val.len()
                ));
            }
            val.clone()
        }
        (PrimitiveLiteral::UInt128(val), crate::iceberg::spec::PrimitiveType::Uuid) => {
            val.to_be_bytes().to_vec()
        }
        (
            PrimitiveLiteral::Int128(val),
            crate::iceberg::spec::PrimitiveType::Decimal { precision, .. },
        ) => {
            let required_bytes = Type::decimal_required_bytes(*precision).map_err(|_| {
                format!("PrimitiveType Decimal must has valid precision but got {precision}")
            })? as usize;
            if !decimal_unscaled_fits_precision(*val, *precision) {
                return Err(format!(
                    "partition decimal literal {val} exceeds decimal precision {precision}"
                ));
            }
            let mut bytes = i128_to_be_bytes_min(*val);
            if bytes.len() > required_bytes {
                return Err(format!(
                    "partition decimal literal {val} requires {} bytes but precision {precision} allows {required_bytes}",
                    bytes.len()
                ));
            }
            bytes.truncate(required_bytes);
            bytes
        }
        _ => {
            return Err(format!(
                "partition literal {literal:?} is incompatible with type {primitive_type:?}"
            ));
        }
    };

    Ok(bytes)
}

fn validate_descriptor_payload_bytes(
    bytes: &[u8],
    primitive_type: &crate::iceberg::spec::PrimitiveType,
    index: usize,
) -> Result<(), IcebergWriteDescriptorError> {
    match primitive_type {
        crate::iceberg::spec::PrimitiveType::Boolean => {
            if bytes.len() != 1 {
                return Err(IcebergWriteDescriptorError::DecodeFailed {
                    index,
                    message: format!(
                        "partition descriptor boolean payload length {} is invalid; expected 1",
                        bytes.len()
                    ),
                });
            }
            if !matches!(bytes[0], 0 | 1) {
                return Err(IcebergWriteDescriptorError::DecodeFailed {
                    index,
                    message: format!(
                        "partition descriptor boolean payload must be 0 or 1, got {}",
                        bytes[0]
                    ),
                });
            }
        }
        crate::iceberg::spec::PrimitiveType::Int
        | crate::iceberg::spec::PrimitiveType::Date
        | crate::iceberg::spec::PrimitiveType::Float => {
            validate_exact_payload_len(bytes, 4, index)?;
        }
        crate::iceberg::spec::PrimitiveType::Time => {
            validate_exact_payload_len(bytes, 8, index)?;
            let value = i64::from_le_bytes(bytes.try_into().map_err(|_| {
                IcebergWriteDescriptorError::DecodeFailed {
                    index,
                    message: "partition descriptor time payload must be exactly 8 bytes"
                        .to_string(),
                }
            })?);
            if !time_micros_in_range(value) {
                return Err(IcebergWriteDescriptorError::DecodeFailed {
                    index,
                    message: format!(
                        "partition time literal {value} is outside valid Iceberg time range 0..={MAX_TIME_MICROS}"
                    ),
                });
            }
        }
        crate::iceberg::spec::PrimitiveType::Long
        | crate::iceberg::spec::PrimitiveType::Timestamp
        | crate::iceberg::spec::PrimitiveType::Timestamptz
        | crate::iceberg::spec::PrimitiveType::TimestampNs
        | crate::iceberg::spec::PrimitiveType::TimestamptzNs
        | crate::iceberg::spec::PrimitiveType::Double => {
            validate_exact_payload_len(bytes, 8, index)?;
        }
        crate::iceberg::spec::PrimitiveType::Uuid => {
            validate_exact_payload_len(bytes, 16, index)?;
        }
        crate::iceberg::spec::PrimitiveType::String
        | crate::iceberg::spec::PrimitiveType::Binary => {}
        crate::iceberg::spec::PrimitiveType::Fixed(len) => {
            let expected =
                usize::try_from(*len).map_err(|_| IcebergWriteDescriptorError::DecodeFailed {
                    index,
                    message: format!("fixed length {len} cannot fit in usize"),
                })?;
            if bytes.len() != expected {
                return Err(IcebergWriteDescriptorError::DecodeFailed {
                    index,
                    message: format!(
                        "partition descriptor payload length {} does not match fixed length {expected}",
                        bytes.len()
                    ),
                });
            }
        }
        crate::iceberg::spec::PrimitiveType::Decimal { precision, .. } => {
            let required_bytes = Type::decimal_required_bytes(*precision).map_err(|_| {
                IcebergWriteDescriptorError::DecodeFailed {
                    index,
                    message: format!(
                        "PrimitiveType Decimal must has valid precision but got {precision}"
                    ),
                }
            })? as usize;
            if bytes.is_empty() || bytes.len() > required_bytes {
                return Err(IcebergWriteDescriptorError::DecodeFailed {
                    index,
                    message: format!(
                        "partition descriptor decimal payload length {} is invalid for precision {precision}; expected 1..={required_bytes}",
                        bytes.len()
                    ),
                });
            }
        }
    }
    Ok(())
}

fn validate_exact_payload_len(
    bytes: &[u8],
    expected: usize,
    index: usize,
) -> Result<(), IcebergWriteDescriptorError> {
    if bytes.len() != expected {
        return Err(IcebergWriteDescriptorError::DecodeFailed {
            index,
            message: format!(
                "partition descriptor payload length {} is not canonical; expected {expected}",
                bytes.len()
            ),
        });
    }
    Ok(())
}

fn time_micros_in_range(value: i64) -> bool {
    (0..=MAX_TIME_MICROS).contains(&value)
}

fn validate_decoded_primitive_literal(
    literal: &PrimitiveLiteral,
    primitive_type: &crate::iceberg::spec::PrimitiveType,
    index: usize,
) -> Result<(), IcebergWriteDescriptorError> {
    if let (
        PrimitiveLiteral::Int128(value),
        crate::iceberg::spec::PrimitiveType::Decimal { precision, .. },
    ) = (literal, primitive_type)
        && !decimal_unscaled_fits_precision(*value, *precision)
    {
        return Err(IcebergWriteDescriptorError::DecodeFailed {
            index,
            message: format!(
                "partition decimal literal {value} exceeds decimal precision {precision}"
            ),
        });
    }
    Ok(())
}

fn decimal_unscaled_fits_precision(value: i128, precision: u32) -> bool {
    let Some(limit) = 10_i128.checked_pow(precision) else {
        return false;
    };
    value < limit && value > -limit
}

fn i128_to_be_bytes_min(value: i128) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let is_negative = value < 0;
    let skip_byte = if is_negative { 0xFF } else { 0x00 };

    let mut start = 0;
    while start < 15 && bytes[start] == skip_byte {
        let next_byte = bytes[start + 1];
        let next_is_negative = (next_byte & 0x80) != 0;
        if next_is_negative == is_negative {
            start += 1;
        } else {
            break;
        }
    }

    bytes[start..].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::TableCreation;
    use crate::iceberg::spec::{
        FormatVersion, Literal, NestedField, PartitionSpec, PrimitiveLiteral, PrimitiveType,
        Schema, TableMetadataBuilder, Transform, Type,
    };
    use std::sync::Arc;

    fn metadata_with_identity_partition() -> TableMetadata {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "region",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .expect("schema");
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(7)
            .add_partition_field("region", "region", Transform::Identity)
            .expect("partition field")
            .build()
            .expect("partition spec");
        let creation = TableCreation::builder()
            .name("t".to_string())
            .location("file:///warehouse/db/t".to_string())
            .schema(schema)
            .partition_spec(spec)
            .format_version(FormatVersion::V2)
            .build();
        TableMetadataBuilder::from_table_creation(creation)
            .expect("table metadata builder")
            .build()
            .expect("table metadata")
            .metadata
    }

    fn metadata_with_single_primitive_partition(
        field_name: &str,
        primitive_type: PrimitiveType,
        spec_id: i32,
    ) -> TableMetadata {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                field_name,
                Type::Primitive(primitive_type),
            ))])
            .build()
            .expect("schema");
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(spec_id)
            .add_partition_field(field_name, field_name, Transform::Identity)
            .expect("partition field")
            .build()
            .expect("partition spec");
        let creation = TableCreation::builder()
            .name("t".to_string())
            .location("file:///warehouse/db/t".to_string())
            .schema(schema)
            .partition_spec(spec)
            .format_version(FormatVersion::V2)
            .build();
        TableMetadataBuilder::from_table_creation(creation)
            .expect("table metadata builder")
            .build()
            .expect("table metadata")
            .metadata
    }

    fn metadata_with_multi_partition_fields() -> TableMetadata {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "flag",
                    Type::Primitive(PrimitiveType::Boolean),
                )),
                Arc::new(NestedField::required(
                    2,
                    "i",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::required(
                    3,
                    "l",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    4,
                    "s",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::required(
                    5,
                    "b",
                    Type::Primitive(PrimitiveType::Binary),
                )),
            ])
            .build()
            .expect("schema");
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(8)
            .add_partition_field("flag", "flag", Transform::Identity)
            .expect("flag partition")
            .add_partition_field("i", "i", Transform::Identity)
            .expect("i partition")
            .add_partition_field("l", "l", Transform::Identity)
            .expect("l partition")
            .add_partition_field("s", "s", Transform::Identity)
            .expect("s partition")
            .add_partition_field("b", "b", Transform::Identity)
            .expect("b partition")
            .build()
            .expect("partition spec");
        let creation = TableCreation::builder()
            .name("t".to_string())
            .location("file:///warehouse/db/t".to_string())
            .schema(schema)
            .partition_spec(spec)
            .format_version(FormatVersion::V2)
            .build();
        TableMetadataBuilder::from_table_creation(creation)
            .expect("table metadata builder")
            .build()
            .expect("table metadata")
            .metadata
    }

    fn metadata_with_decimal_partition() -> TableMetadata {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "amount",
                Type::Primitive(PrimitiveType::Decimal {
                    precision: 3,
                    scale: 0,
                }),
            ))])
            .build()
            .expect("schema");
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(9)
            .add_partition_field("amount", "amount", Transform::Identity)
            .expect("amount partition")
            .build()
            .expect("partition spec");
        let creation = TableCreation::builder()
            .name("t".to_string())
            .location("file:///warehouse/db/t".to_string())
            .schema(schema)
            .partition_spec(spec)
            .format_version(FormatVersion::V2)
            .build();
        TableMetadataBuilder::from_table_creation(creation)
            .expect("table metadata builder")
            .build()
            .expect("table metadata")
            .metadata
    }

    fn metadata_with_fixed_partition() -> TableMetadata {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "token",
                Type::Primitive(PrimitiveType::Fixed(4)),
            ))])
            .build()
            .expect("schema");
        let spec = PartitionSpec::builder(schema.clone())
            .with_spec_id(10)
            .add_partition_field("token", "token", Transform::Identity)
            .expect("token partition")
            .build()
            .expect("partition spec");
        let creation = TableCreation::builder()
            .name("t".to_string())
            .location("file:///warehouse/db/t".to_string())
            .schema(schema)
            .partition_spec(spec)
            .format_version(FormatVersion::V2)
            .build();
        TableMetadataBuilder::from_table_creation(creation)
            .expect("table metadata builder")
            .build()
            .expect("table metadata")
            .metadata
    }

    fn raw_descriptor(
        values: impl IntoIterator<Item = IcebergPartitionValueDescriptor>,
    ) -> Option<IcebergPartitionDescriptor> {
        Some(IcebergPartitionDescriptor {
            values: values.into_iter().collect(),
        })
    }

    fn raw_value(is_null: bool, datum_bytes: Option<Vec<u8>>) -> IcebergPartitionValueDescriptor {
        IcebergPartitionValueDescriptor {
            is_null,
            datum_bytes,
        }
    }

    #[test]
    fn descriptor_encode_returns_internal_values() {
        let metadata = metadata_with_identity_partition();
        let spec_id = metadata.default_partition_spec_id();
        let values = Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::String(
            "west".to_string(),
        )))]);

        let desc =
            encode_partition_descriptor(&values, spec_id, &metadata).expect("encode descriptor");

        assert_eq!(desc.values.len(), 1);
        assert!(!desc.values[0].is_null);
        assert_eq!(
            desc.values[0].datum_bytes.as_deref(),
            Some(b"west".as_slice())
        );
    }

    #[test]
    fn descriptor_round_trips_identity_partition() {
        let metadata = metadata_with_identity_partition();
        let spec_id = metadata.default_partition_spec_id();
        let values = Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::String(
            "us west".to_string(),
        )))]);

        let desc =
            encode_partition_descriptor(&values, spec_id, &metadata).expect("encode descriptor");
        let decoded =
            decode_partition_descriptor(Some(desc), spec_id, &metadata).expect("decode descriptor");

        assert_eq!(decoded, values);
    }

    #[test]
    fn descriptor_round_trips_common_primitive_literals() {
        let metadata = metadata_with_multi_partition_fields();
        let spec_id = metadata.default_partition_spec_id();
        let values = Struct::from_iter([
            Some(Literal::Primitive(PrimitiveLiteral::Boolean(true))),
            Some(Literal::Primitive(PrimitiveLiteral::Int(7))),
            Some(Literal::Primitive(PrimitiveLiteral::Long(9))),
            Some(Literal::Primitive(PrimitiveLiteral::String(
                "west".to_string(),
            ))),
            Some(Literal::Primitive(PrimitiveLiteral::Binary(vec![1, 2, 3]))),
        ]);

        let desc =
            encode_partition_descriptor(&values, spec_id, &metadata).expect("encode descriptor");
        let decoded =
            decode_partition_descriptor(Some(desc), spec_id, &metadata).expect("decode descriptor");

        assert_eq!(decoded, values);
    }

    #[test]
    fn descriptor_round_trips_decimal_partition_value() {
        let metadata = metadata_with_decimal_partition();
        let spec_id = metadata.default_partition_spec_id();
        let values = Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::Int128(-123)))]);

        let desc =
            encode_partition_descriptor(&values, spec_id, &metadata).expect("encode descriptor");
        let decoded =
            decode_partition_descriptor(Some(desc), spec_id, &metadata).expect("decode descriptor");

        assert_eq!(decoded, values);
    }

    #[test]
    fn descriptor_round_trips_fixed_partition_value() {
        let metadata = metadata_with_fixed_partition();
        let spec_id = metadata.default_partition_spec_id();
        let values = Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::Binary(vec![
            1, 2, 3, 4,
        ])))]);

        let desc =
            encode_partition_descriptor(&values, spec_id, &metadata).expect("encode descriptor");
        let decoded =
            decode_partition_descriptor(Some(desc), spec_id, &metadata).expect("decode descriptor");

        assert_eq!(decoded, values);
    }

    #[test]
    fn descriptor_rejects_decimal_precision_overflow() {
        let metadata = metadata_with_decimal_partition();
        let spec_id = metadata.default_partition_spec_id();
        let values = Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::Int128(1000)))]);

        let err = encode_partition_descriptor(&values, spec_id, &metadata)
            .expect_err("expected precision overflow");

        assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
        assert!(err.to_string().contains("exceeds decimal precision"));
    }

    #[test]
    fn descriptor_decodes_valid_decimal_precision_boundaries() {
        let metadata = metadata_with_decimal_partition();
        let spec_id = metadata.default_partition_spec_id();
        for value in [999_i128, -999_i128] {
            let desc = encode_partition_descriptor(
                &Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::Int128(value)))]),
                spec_id,
                &metadata,
            )
            .expect("encode descriptor");

            let decoded =
                decode_partition_descriptor(Some(desc), spec_id, &metadata).expect("decode");

            assert_eq!(
                decoded,
                Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::Int128(value)))])
            );
        }
    }

    #[test]
    fn descriptor_decodes_minimal_negative_decimal_payload() {
        let metadata = metadata_with_decimal_partition();
        let spec_id = metadata.default_partition_spec_id();
        let desc = raw_descriptor([raw_value(false, Some(vec![0xff]))]);

        let decoded = decode_partition_descriptor(desc, spec_id, &metadata)
            .expect("minimal negative payload should decode");

        assert_eq!(
            decoded,
            Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::Int128(-1)))])
        );
    }

    #[test]
    fn descriptor_rejects_decoded_decimal_precision_overflow() {
        let metadata = metadata_with_decimal_partition();
        let spec_id = metadata.default_partition_spec_id();
        for value in [1000_i128, -1000_i128] {
            let bytes = i128_to_be_bytes_min(value);
            let desc = raw_descriptor([raw_value(false, Some(bytes))]);

            let err = decode_partition_descriptor(desc, spec_id, &metadata)
                .expect_err("expected precision overflow");

            assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
            assert!(
                err.to_string().contains("exceeds decimal precision"),
                "value {value} should be rejected, got: {err}"
            );
        }
    }

    #[test]
    fn descriptor_rejects_malformed_decimal_payload_width() {
        let metadata = metadata_with_decimal_partition();
        let spec_id = metadata.default_partition_spec_id();
        for bytes in [vec![], vec![0x00, 0x00, 0x01], vec![0xff, 0xff, 0xff]] {
            let desc = raw_descriptor([raw_value(false, Some(bytes.clone()))]);

            let err = decode_partition_descriptor(desc, spec_id, &metadata)
                .expect_err("expected malformed decimal payload");

            assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
            assert!(
                err.to_string().contains("decimal payload"),
                "payload {bytes:?} should be rejected, got: {err}"
            );
        }
    }

    #[test]
    fn descriptor_rejects_fixed_length_mismatch() {
        let metadata = metadata_with_fixed_partition();
        let spec_id = metadata.default_partition_spec_id();
        let values = Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::Binary(vec![
            1, 2, 3,
        ])))]);

        let err = encode_partition_descriptor(&values, spec_id, &metadata)
            .expect_err("expected fixed length mismatch");

        assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
        assert!(err.to_string().contains("fixed length"));
    }

    #[test]
    fn descriptor_rejects_decoded_fixed_length_mismatch() {
        let metadata = metadata_with_fixed_partition();
        let spec_id = metadata.default_partition_spec_id();
        let desc = raw_descriptor([raw_value(false, Some(vec![1, 2, 3]))]);

        let err = decode_partition_descriptor(desc, spec_id, &metadata)
            .expect_err("expected fixed length mismatch");

        assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
        assert!(err.to_string().contains("fixed length"));
    }

    #[test]
    fn descriptor_round_trips_null_partition_value() {
        let metadata = metadata_with_identity_partition();
        let spec_id = metadata.default_partition_spec_id();
        let values = Struct::from_iter([None]);

        let desc =
            encode_partition_descriptor(&values, spec_id, &metadata).expect("encode descriptor");
        let decoded =
            decode_partition_descriptor(Some(desc), spec_id, &metadata).expect("decode descriptor");

        assert_eq!(decoded, values);
    }

    #[test]
    fn descriptor_rejects_missing_payload_for_non_null_value() {
        let metadata = metadata_with_identity_partition();
        let spec_id = metadata.default_partition_spec_id();
        let desc = raw_descriptor([raw_value(false, None)]);

        let err =
            decode_partition_descriptor(desc, spec_id, &metadata).expect_err("expected error");

        assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
        assert!(err.to_string().contains("has no payload"));
    }

    #[test]
    fn descriptor_rejects_payload_for_null_value() {
        let metadata = metadata_with_identity_partition();
        let spec_id = metadata.default_partition_spec_id();
        let desc = raw_descriptor([raw_value(true, Some(b"west".to_vec()))]);

        let err = decode_partition_descriptor(desc, spec_id, &metadata)
            .expect_err("expected malformed null payload");

        assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
        assert!(
            err.to_string().contains("null value"),
            "null value payload should be rejected, got: {err}"
        );
    }

    #[test]
    fn descriptor_rejects_malformed_boolean_payload() {
        let metadata = metadata_with_single_primitive_partition("flag", PrimitiveType::Boolean, 11);
        let spec_id = metadata.default_partition_spec_id();
        for bytes in [vec![], vec![2], vec![0, 0]] {
            let desc = raw_descriptor([raw_value(false, Some(bytes.clone()))]);

            let err = decode_partition_descriptor(desc, spec_id, &metadata)
                .expect_err("expected malformed boolean payload");

            assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
            assert!(
                err.to_string().contains("boolean payload"),
                "payload {bytes:?} should be rejected, got: {err}"
            );
        }
    }

    #[test]
    fn descriptor_rejects_promoted_long_payload() {
        let metadata = metadata_with_single_primitive_partition("l", PrimitiveType::Long, 12);
        let spec_id = metadata.default_partition_spec_id();
        let desc = raw_descriptor([raw_value(false, Some(7_i32.to_le_bytes().to_vec()))]);

        let err = decode_partition_descriptor(desc, spec_id, &metadata)
            .expect_err("expected non-canonical long payload");

        assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
        assert!(
            err.to_string().contains("payload length"),
            "4-byte long payload should be rejected, got: {err}"
        );
    }

    #[test]
    fn descriptor_rejects_promoted_double_payload() {
        let metadata = metadata_with_single_primitive_partition("d", PrimitiveType::Double, 13);
        let spec_id = metadata.default_partition_spec_id();
        let desc = raw_descriptor([raw_value(false, Some(7.0_f32.to_le_bytes().to_vec()))]);

        let err = decode_partition_descriptor(desc, spec_id, &metadata)
            .expect_err("expected non-canonical double payload");

        assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
        assert!(
            err.to_string().contains("payload length"),
            "4-byte double payload should be rejected, got: {err}"
        );
    }

    #[test]
    fn descriptor_round_trips_valid_time_boundaries() {
        let metadata = metadata_with_single_primitive_partition("t", PrimitiveType::Time, 14);
        let spec_id = metadata.default_partition_spec_id();
        for value in [0_i64, MAX_TIME_MICROS] {
            let values =
                Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::Long(value)))]);

            let desc =
                encode_partition_descriptor(&values, spec_id, &metadata).expect("encode time");
            let decoded =
                decode_partition_descriptor(Some(desc), spec_id, &metadata).expect("decode time");

            assert_eq!(decoded, values);
        }
    }

    #[test]
    fn descriptor_rejects_out_of_range_time_literal() {
        let metadata = metadata_with_single_primitive_partition("t", PrimitiveType::Time, 14);
        let spec_id = metadata.default_partition_spec_id();
        for value in [-1_i64, MAX_TIME_MICROS + 1] {
            let values =
                Struct::from_iter([Some(Literal::Primitive(PrimitiveLiteral::Long(value)))]);

            let err = encode_partition_descriptor(&values, spec_id, &metadata)
                .expect_err("expected out-of-range time");

            assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
            assert!(
                err.to_string().contains("time literal"),
                "value {value} should be rejected, got: {err}"
            );
        }
    }

    #[test]
    fn descriptor_rejects_decoded_out_of_range_time_payload() {
        let metadata = metadata_with_single_primitive_partition("t", PrimitiveType::Time, 14);
        let spec_id = metadata.default_partition_spec_id();
        for value in [-1_i64, MAX_TIME_MICROS + 1] {
            let desc = raw_descriptor([raw_value(false, Some(value.to_le_bytes().to_vec()))]);

            let err = decode_partition_descriptor(desc, spec_id, &metadata)
                .expect_err("expected out-of-range time payload");

            assert_eq!(err.code(), "IcebergWriteDescriptorMismatch");
            assert!(
                err.to_string().contains("time literal"),
                "value {value} should be rejected, got: {err}"
            );
        }
    }

    #[test]
    fn descriptor_rejects_missing_descriptor() {
        let metadata = metadata_with_identity_partition();
        let spec_id = metadata.default_partition_spec_id();

        let err =
            decode_partition_descriptor(None, spec_id, &metadata).expect_err("expected error");

        assert_eq!(err, IcebergWriteDescriptorError::MissingDescriptor);
    }

    #[test]
    fn descriptor_rejects_unknown_partition_spec_id() {
        let metadata = metadata_with_identity_partition();
        let desc = raw_descriptor([]);

        let err = decode_partition_descriptor(desc, 99, &metadata).expect_err("expected error");

        assert_eq!(
            err,
            IcebergWriteDescriptorError::UnknownPartitionSpec { spec_id: 99 }
        );
    }

    #[test]
    fn descriptor_rejects_field_count_mismatch() {
        let metadata = metadata_with_identity_partition();
        let spec_id = metadata.default_partition_spec_id();
        let desc = raw_descriptor([]);

        let err =
            decode_partition_descriptor(desc, spec_id, &metadata).expect_err("expected error");

        assert_eq!(
            err,
            IcebergWriteDescriptorError::FieldCountMismatch {
                expected: 1,
                actual: 0,
            }
        );
    }
}
