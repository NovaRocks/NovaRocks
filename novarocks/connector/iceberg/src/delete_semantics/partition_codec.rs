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

//! Lossless provider-private transport of stored partition values.
//!
//! This is not Iceberg's external JSON value format. Its floating-point bit
//! representation preserves NaN, infinities and signed zero; byte arrays do
//! not depend on a hexadecimal formatter. The independently pinned spec and
//! schema supply field order and types. The private provider revision cuts
//! old payloads rather than interpreting a second tuple representation.

use serde::{Deserialize, Serialize};

use crate::iceberg::spec::{Literal, PartitionSpec, PrimitiveLiteral, Schema, Struct, StructType};

use super::{
    CanonicalScalar, DeleteSemanticsError, DeleteSemanticsErrorKind, Result, TypedPartition,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TupleEnvelope {
    version: u8,
    values: Vec<Option<EncodedScalar>>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum EncodedScalar {
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(u32),
    Double(u64),
    Decimal(String),
    Uuid(String),
    String(String),
    Binary(Vec<u8>),
}

impl From<&CanonicalScalar> for EncodedScalar {
    fn from(value: &CanonicalScalar) -> Self {
        match value {
            CanonicalScalar::Boolean(value) => Self::Boolean(*value),
            CanonicalScalar::Int(value) => Self::Int(*value),
            CanonicalScalar::Long(value) => Self::Long(*value),
            CanonicalScalar::Float(bits) => Self::Float(*bits),
            CanonicalScalar::Double(bits) => Self::Double(*bits),
            CanonicalScalar::Decimal(value) => Self::Decimal(value.to_string()),
            CanonicalScalar::Uuid(value) => Self::Uuid(format!("{value:032x}")),
            CanonicalScalar::String(value) => Self::String(value.to_string()),
            CanonicalScalar::Binary(value) => Self::Binary(value.to_vec()),
        }
    }
}

fn invalid(message: impl Into<String>) -> DeleteSemanticsError {
    DeleteSemanticsError::new(DeleteSemanticsErrorKind::InvalidPartition, message)
}

impl EncodedScalar {
    fn into_literal(self) -> Result<Literal> {
        Ok(match self {
            Self::Boolean(value) => Literal::bool(value),
            Self::Int(value) => Literal::int(value),
            Self::Long(value) => Literal::long(value),
            Self::Float(bits) => Literal::float(f32::from_bits(bits)),
            Self::Double(bits) => Literal::double(f64::from_bits(bits)),
            Self::Decimal(value) => Literal::Primitive(PrimitiveLiteral::Int128(
                value
                    .parse()
                    .map_err(|_| invalid("invalid partition decimal coefficient"))?,
            )),
            Self::Uuid(value) => {
                if value.len() != 32 {
                    return Err(invalid("partition UUID must contain 32 hexadecimal digits"));
                }
                Literal::Primitive(PrimitiveLiteral::UInt128(
                    u128::from_str_radix(&value, 16)
                        .map_err(|_| invalid("invalid partition UUID"))?,
                ))
            }
            Self::String(value) => Literal::string(value),
            Self::Binary(value) => Literal::binary(value),
        })
    }
}

impl TypedPartition {
    /// All represented numbers are bounded integers; decimal and UUID values
    /// are strings. No non-finite JSON number or custom serializer can fail.
    pub fn to_json_string(&self) -> String {
        serde_json::to_string(&TupleEnvelope {
            version: 1,
            values: self
                .values()
                .iter()
                .map(|value| value.as_ref().map(EncodedScalar::from))
                .collect(),
        })
        .expect("canonical partition tuple contains only JSON-supported scalars")
    }
}

/// Decode a stored tuple against the separately pinned field/type binding.
/// Callers enforce the private payload's byte budget before invoking this
/// pure codec. Missing tuples, old external JSON and unknown versions fail.
pub fn decode_partition_data_json(
    spec: &PartitionSpec,
    schema: &Schema,
    json: &str,
) -> Result<Struct> {
    let partition_type = spec
        .partition_type(schema)
        .map_err(|e| invalid(e.to_string()))?;
    decode_partition_data_json_with_type(spec, &partition_type, json)
}

pub fn decode_partition_data_json_with_type(
    spec: &PartitionSpec,
    partition_type: &StructType,
    json: &str,
) -> Result<Struct> {
    let envelope: TupleEnvelope = serde_json::from_str(json)
        .map_err(|error| invalid(format!("invalid canonical partition tuple: {error}")))?;
    if envelope.version != 1 {
        return Err(invalid("unsupported canonical partition tuple version"));
    }
    let values = envelope
        .values
        .into_iter()
        .map(|value| value.map(EncodedScalar::into_literal).transpose())
        .collect::<Result<Struct>>()?;
    TypedPartition::bind_type(spec, partition_type, &values)?;
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{NestedField, PrimitiveType, Type};

    fn binding(primitive: PrimitiveType) -> (Schema, PartitionSpec) {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "key", Type::Primitive(primitive)).into(),
            ])
            .build()
            .unwrap();
        let spec = serde_json::from_str(
            r#"{"spec-id":2,"fields":[{"source-id":1,"field-id":1000,"name":"key","transform":"identity"}]}"#,
        ).unwrap();
        (schema, spec)
    }

    fn round_trip(primitive: PrimitiveType, value: Literal) -> TypedPartition {
        let (schema, spec) = binding(primitive);
        let values: Struct = [Some(value)].into_iter().collect();
        let original = TypedPartition::bind(&spec, &schema, &values).unwrap();
        let json = original.to_json_string();
        let received = decode_partition_data_json(&spec, &schema, &json).unwrap();
        let received = TypedPartition::bind(&spec, &schema, &received).unwrap();
        assert_eq!(original, received);
        assert_eq!(json, received.to_json_string());
        received
    }

    #[test]
    fn nonfinite_and_signed_zero_partitions_are_never_null_or_collapsed() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0, 0.0, -1.75] {
            let partition = round_trip(PrimitiveType::Double, Literal::double(value));
            assert!(partition.values()[0].is_some());
        }
        assert_ne!(
            round_trip(PrimitiveType::Double, Literal::double(-0.0)),
            round_trip(PrimitiveType::Double, Literal::double(0.0)),
        );
        round_trip(PrimitiveType::Float, Literal::float(f32::NAN));
    }

    #[test]
    fn binary_zero_octets_decimal_uuid_and_temporal_values_round_trip() {
        round_trip(PrimitiveType::Binary, Literal::binary([0, 1, 15, 16, 255]));
        round_trip(
            PrimitiveType::Fixed(5),
            Literal::binary([0, 1, 15, 16, 255]),
        );
        round_trip(
            PrimitiveType::Decimal {
                precision: 38,
                scale: 9,
            },
            Literal::Primitive(PrimitiveLiteral::Int128(-123456789012345678901234567890)),
        );
        round_trip(
            PrimitiveType::Uuid,
            Literal::Primitive(PrimitiveLiteral::UInt128(u128::MAX)),
        );
        round_trip(PrimitiveType::Date, Literal::int(-27));
        round_trip(
            PrimitiveType::TimestampNs,
            Literal::long(-1_234_567_890_123_i64),
        );
    }

    #[test]
    fn null_and_empty_tuples_are_distinct_from_missing_or_legacy_payloads() {
        let (schema, spec) = binding(PrimitiveType::Long);
        let null: Struct = [None].into_iter().collect();
        let partition = TypedPartition::bind(&spec, &schema, &null).unwrap();
        let restored =
            decode_partition_data_json(&spec, &schema, &partition.to_json_string()).unwrap();
        assert!(restored.fields()[0].is_none());
        assert!(decode_partition_data_json(&spec, &schema, "{}").is_err());
        assert!(decode_partition_data_json(&spec, &schema, r#"{"1000":7}"#).is_err());
        assert!(
            decode_partition_data_json(&spec, &schema, r#"{"version":2,"values":[]}"#).is_err()
        );
        assert!(
            decode_partition_data_json(&spec, &schema, r#"{"version":1,"values":[]}"#).is_err()
        );
        let spec = PartitionSpec::unpartition_spec();
        let empty = TypedPartition::bind(&spec, &schema, &Struct::empty()).unwrap();
        assert!(
            decode_partition_data_json(&spec, &schema, &empty.to_json_string())
                .unwrap()
                .fields()
                .is_empty()
        );
    }

    #[test]
    fn date_unit_promotion_is_rejected_only_when_a_value_requires_conversion() {
        let (old_schema, spec) = binding(PrimitiveType::Date);
        let old_values = Struct::from_iter([Some(Literal::int(-7))]);
        let old = TypedPartition::bind(&spec, &old_schema, &old_values).unwrap();
        for target in [PrimitiveType::Timestamp, PrimitiveType::TimestampNs] {
            let (new_schema, _) = binding(target);
            let storage_type = spec.partition_type(&new_schema).unwrap();
            let error =
                decode_partition_data_json_with_type(&spec, &storage_type, &old.to_json_string())
                    .unwrap_err();
            assert_eq!(error.kind, DeleteSemanticsErrorKind::UnsupportedPromotion);

            let null = Struct::from_iter([None]);
            let old_null = TypedPartition::bind(&spec, &old_schema, &null).unwrap();
            assert_eq!(
                decode_partition_data_json_with_type(
                    &spec,
                    &storage_type,
                    &old_null.to_json_string(),
                )
                .unwrap(),
                null
            );
        }
    }

    #[test]
    fn duplicate_fields_unknown_tags_bad_width_and_wrong_types_fail() {
        let (schema, spec) = binding(PrimitiveType::Long);
        for json in [
            r#"{"version":1,"version":1,"values":[{"long":7}]}"#,
            r#"{"version":1,"values":[{"long":7,"long":8}]}"#,
            r#"{"version":1,"values":[{"unknown":7}]}"#,
            r#"{"version":1,"values":[{"float":4294967296}]}"#,
            r#"{"version":1,"values":[{"boolean":true}]}"#,
        ] {
            assert!(
                decode_partition_data_json(&spec, &schema, json).is_err(),
                "{json}"
            );
        }
    }
}
