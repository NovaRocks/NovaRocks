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

//! Observed candidate indexing for exact Arrow type identity.
//! Fingerprints select candidates; exact observed comparison decides identity.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use arrow_schema::{DataType, Field};

use crate::{
    ArrowTypeProfile, ValueTypeError, ValueTypeVisit,
    validate_value_type_structure_with_profile_observed,
};

/// Hashes the actual type and Field facts without an opaque recursive Hash.
/// Metadata order is immaterial, while field order, dictionary identity and
/// all primitive parameters remain candidate inputs. No fingerprint proves
/// equality, and scratch/hasher allocation is not a host memory grant.
pub fn arrow_data_type_fingerprint_observed<E: From<ValueTypeError>>(
    ty: &DataType,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<u64, E> {
    arrow_data_type_fingerprint_with_profile_observed(ty, ArrowTypeProfile::Physical, observe)
}

/// Hash the same exact type/Field facts under the actual boundary's declared
/// profile. Typed immutable layouts select CanonicalValue; this does not alter
/// the original raw/constant/provider Physical policy or create encoding rights.
pub fn arrow_data_type_fingerprint_with_profile_observed<E: From<ValueTypeError>>(
    ty: &DataType,
    profile: ArrowTypeProfile,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<u64, E> {
    let mut hash = DefaultHasher::new();
    validate_value_type_structure_with_profile_observed::<E>(ty, profile, |visit| {
        observe()?;
        match visit {
            ValueTypeVisit::ChildEdge(_) => 0u8.hash(&mut hash),
            ValueTypeVisit::Field(field) => {
                1u8.hash(&mut hash);
                field_fingerprint(field, &mut hash, observe)?;
            }
            ValueTypeVisit::TypeNode(ty) => {
                2u8.hash(&mut hash);
                std::mem::discriminant(ty).hash(&mut hash);
                match ty {
                    DataType::Timestamp(unit, timezone) => {
                        unit.hash(&mut hash);
                        timezone.is_some().hash(&mut hash);
                        if let Some(timezone) = timezone {
                            bytes(timezone.as_bytes(), &mut hash, observe)?;
                        }
                    }
                    DataType::Time32(unit) | DataType::Time64(unit) | DataType::Duration(unit) => {
                        unit.hash(&mut hash);
                    }
                    DataType::Interval(unit) => unit.hash(&mut hash),
                    DataType::FixedSizeBinary(width) | DataType::FixedSizeList(_, width) => {
                        width.hash(&mut hash);
                    }
                    DataType::Decimal32(precision, scale)
                    | DataType::Decimal64(precision, scale)
                    | DataType::Decimal128(precision, scale)
                    | DataType::Decimal256(precision, scale) => {
                        precision.hash(&mut hash);
                        scale.hash(&mut hash);
                    }
                    DataType::Struct(fields) => fields.len().hash(&mut hash),
                    DataType::Union(fields, mode) => {
                        mode.hash(&mut hash);
                        fields.len().hash(&mut hash);
                        for (id, _) in fields.iter() {
                            observe()?;
                            id.hash(&mut hash);
                        }
                    }
                    DataType::Map(_, sorted) => sorted.hash(&mut hash),
                    DataType::Null
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Binary
                    | DataType::LargeBinary
                    | DataType::BinaryView
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View
                    | DataType::List(_)
                    | DataType::ListView(_)
                    | DataType::LargeList(_)
                    | DataType::LargeListView(_)
                    | DataType::Dictionary(_, _)
                    | DataType::RunEndEncoded(_, _) => {}
                }
            }
        }
        Ok(())
    })?;
    Ok(hash.finish())
}

fn bytes<E>(
    value: &[u8],
    hash: &mut impl Hasher,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    value.len().hash(hash);
    for chunk in value.chunks(1024) {
        observe()?;
        hash.write(chunk);
    }
    Ok(())
}

fn field_fingerprint<E>(
    field: &Field,
    hash: &mut impl Hasher,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<(), E> {
    bytes(field.name().as_bytes(), hash, observe)?;
    field.is_nullable().hash(hash);
    #[allow(deprecated)]
    field.dict_id().hash(hash);
    field.dict_is_ordered().hash(hash);
    field.metadata().len().hash(hash);
    // An unordered multiset digest avoids sorting/cloning metadata. Exact
    // comparison still checks every key/value after a candidate collision.
    let mut metadata = 0u64;
    for (key, value) in field.metadata() {
        observe()?;
        let mut entry = DefaultHasher::new();
        bytes(key.as_bytes(), &mut entry, observe)?;
        bytes(value.as_bytes(), &mut entry, observe)?;
        metadata = metadata.wrapping_add(entry.finish());
    }
    metadata.hash(hash);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn fingerprint(ty: &DataType) -> u64 {
        arrow_data_type_fingerprint_observed::<ValueTypeError>(ty, &mut || Ok(())).unwrap()
    }

    #[test]
    #[allow(deprecated)] // Exact frozen dictionary IDs remain part of identity.
    fn metadata_order_is_irrelevant_and_exact_field_facts_remain_inputs() {
        let field = |entries: Vec<(&str, &str)>| {
            Field::new_dict(
                "item",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
                9,
                true,
            )
            .with_metadata(
                entries
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<HashMap<_, _>>(),
            )
        };
        let a = DataType::List(Arc::new(field(vec![("a", "1"), ("b", "2")])));
        let b = DataType::List(Arc::new(field(vec![("b", "2"), ("a", "1")])));
        assert!(crate::arrow_data_types_exact(&a, &b));
        assert_eq!(fingerprint(&a), fingerprint(&b));
        let changed = DataType::List(Arc::new(field(vec![("a", "2"), ("b", "1")])));
        assert_ne!(fingerprint(&a), fingerprint(&changed));
        #[allow(deprecated)]
        let changed_id = DataType::List(Arc::new(
            Field::new_dict(
                "item",
                DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
                false,
                10,
                true,
            )
            .with_metadata(field(vec![("a", "1"), ("b", "2")]).metadata().clone()),
        ));
        assert_ne!(fingerprint(&a), fingerprint(&changed_id));
    }

    #[test]
    fn primitive_parameters_child_order_and_union_tags_are_observed() {
        let fields = vec![
            Field::new("x", DataType::Int64, false),
            Field::new("y", DataType::Utf8, true),
        ];
        let a = DataType::Struct(fields.clone().into());
        let b = DataType::Struct(fields.into_iter().rev().collect());
        assert_ne!(fingerprint(&a), fingerprint(&b));
        assert_ne!(
            fingerprint(&DataType::Decimal128(12, 2)),
            fingerprint(&DataType::Decimal128(12, 3))
        );
        assert_ne!(
            fingerprint(&DataType::Timestamp(arrow_schema::TimeUnit::Second, None)),
            fingerprint(&DataType::Timestamp(
                arrow_schema::TimeUnit::Second,
                Some("UTC".into())
            ))
        );
        let union = |id| {
            DataType::Union(
                arrow_schema::UnionFields::try_new(
                    vec![id],
                    vec![Field::new("v", DataType::Int64, false)],
                )
                .unwrap(),
                arrow_schema::UnionMode::Dense,
            )
        };
        assert_ne!(fingerprint(&union(1)), fingerprint(&union(2)));
    }

    #[test]
    fn actual_wide_and_long_metadata_work_can_be_refused_without_a_fingerprint() {
        #[derive(Debug, Eq, PartialEq)]
        enum Failure {
            Type(ValueTypeError),
            Control(crate::CompileControlError),
        }
        impl From<ValueTypeError> for Failure {
            fn from(value: ValueTypeError) -> Self {
                Self::Type(value)
            }
        }
        let ty = DataType::Struct(
            (0..300)
                .map(|i| Field::new(i.to_string(), DataType::Int64, false))
                .collect(),
        );
        for cause in [
            crate::CompileControlError::Cancelled,
            crate::CompileControlError::DeadlineExceeded,
            crate::CompileControlError::ResourceExhausted,
        ] {
            let mut count = 0;
            let error = arrow_data_type_fingerprint_observed::<Failure>(&ty, &mut || {
                count += 1;
                if count == 256 {
                    Err(Failure::Control(cause.clone()))
                } else {
                    Ok(())
                }
            });
            assert_eq!(error, Err(Failure::Control(cause)));
            assert_eq!(count, 256);
        }
        let long = DataType::List(Arc::new(
            Field::new("item", DataType::Int64, false).with_metadata(HashMap::from([(
                "payload".to_string(),
                "x".repeat(300 * 1024),
            )])),
        ));
        let mut count = 0;
        let result = arrow_data_type_fingerprint_observed::<ValueTypeError>(&long, &mut || {
            count += 1;
            Ok(())
        });
        assert!(result.is_ok());
        assert!(count >= 300);
    }
}
