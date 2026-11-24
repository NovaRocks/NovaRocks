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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;

use arrow_schema::DataType;

use crate::LARGEINT_BYTE_WIDTH;

/// Fixed domains decorrelate instance selection from local driver selection.
/// Each level uses the same domain across all sources; no Task identity enters.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PartitionHashDomain {
    InstanceV1,
    DriverV1,
}

impl PartitionHashDomain {
    pub const fn stable_name(self) -> &'static str {
        match self {
            Self::InstanceV1 => "novarocks.partition.instance.v1",
            Self::DriverV1 => "novarocks.partition.driver.v1",
        }
    }
    pub const fn mix(self, canonical_hash: u64) -> u64 {
        let domain = match self {
            Self::InstanceV1 => 0x3b8d_9f26_4a71_c5e0,
            Self::DriverV1 => 0xa5c1_68e3_7d02_b94f,
        };
        avalanche_hash_u64(canonical_hash ^ domain)
    }
}

/// Full-width avalanche shared with native key hashing. This freezes the
/// existing execution author; a low-bit XOR is not a substitute for it.
pub const fn avalanche_hash_u64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

/// Complete row-hash identity shared by planner, codec and execution.
///
/// A variant freezes value canonicalization, NULL handling, multi-column
/// combination and partition reduction. Any change requires a new variant.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PartitionHashAlgorithm {
    /// Native exchange FNV-based row combination followed by unsigned modulo.
    NativeExchangeV1,
    /// Ordinary exchange over canonical MAP identity, followed by unsigned modulo.
    NativeExchangeV2,
    /// The same canonical row encoding and combination as NativeExchangeV1,
    /// followed by InstanceV1 or DriverV1 full-width domain mixing before
    /// unsigned modulo at that level. Source and Task identities never enter.
    NativeHierarchicalV1,
    /// Native bucket-shuffle CRC32 combination followed by bucket modulo.
    NativeBucketCrc32V1,
}

impl PartitionHashAlgorithm {
    pub const fn stable_name(self) -> &'static str {
        match self {
            Self::NativeExchangeV1 => "novarocks.native-exchange.v1",
            Self::NativeExchangeV2 => "novarocks.native-exchange.v2",
            Self::NativeHierarchicalV1 => "novarocks.native-hierarchical.v1",
            Self::NativeBucketCrc32V1 => "novarocks.native-bucket-crc32.v1",
        }
    }

    pub const fn nulls_equal(self) -> bool {
        match self {
            Self::NativeExchangeV1
            | Self::NativeExchangeV2
            | Self::NativeHierarchicalV1
            | Self::NativeBucketCrc32V1 => true,
        }
    }

    /// Ordinary grouped and set keys containing a MAP need its unordered identity.
    pub fn for_grouping_keys<'a>(types: impl IntoIterator<Item = &'a DataType>) -> Self {
        if types.into_iter().any(contains_map) {
            Self::NativeExchangeV2
        } else {
            Self::NativeExchangeV1
        }
    }

    /// Reports the exact Arrow key domain of the frozen algorithm.
    ///
    /// Ordinary V1 and hierarchical V1 retain positional MAP encoding.
    /// Ordinary V2 hashes MAP entries by their canonical unordered identity. Floating-point
    /// keys belong to the native exchange, whose encoding folds signed zero
    /// and NaN payloads. They do not belong to the bucket algorithm: Iceberg's
    /// bucket transform is undefined for `float` and `double`, and this is its
    /// identity, not ours to widen.
    ///
    /// The exchange reaches nested keys through that same encoding, so a list,
    /// struct or map is a key exactly when every member it reaches is one. A
    /// bucket stays at the two list shapes Iceberg's transform names. Unsigned
    /// and dictionary-encoded types remain outside v1 until their canonical
    /// encoding is frozen under a new algorithm identity.
    pub fn supports_partition_value_type(self, value_type: &crate::FunctionValueType) -> bool {
        self.supports_partition_binding(value_type.value_type(), value_type.data_type())
    }

    pub fn supports_partition_binding(
        self,
        semantic: &crate::ValueTypeRef,
        data_type: &DataType,
    ) -> bool {
        self.supports_partition_key(data_type)
            || matches!(
                self,
                Self::NativeExchangeV1 | Self::NativeExchangeV2 | Self::NativeHierarchicalV1
            ) && canonical_temporal_key(semantic, data_type)
    }

    pub fn supports_partition_key(self, data_type: &DataType) -> bool {
        match self {
            Self::NativeExchangeV1 | Self::NativeExchangeV2 | Self::NativeHierarchicalV1 => {
                canonical_key_leaf(data_type)
                    || matches!(
                        data_type,
                        DataType::LargeUtf8 | DataType::Float32 | DataType::Float64
                    )
                    || canonical_key_nesting(data_type)
            }
            Self::NativeBucketCrc32V1 => {
                canonical_key_leaf(data_type)
                    || matches!(data_type, DataType::LargeUtf8)
                    || matches!(
                        data_type,
                        DataType::List(field)
                            if matches!(field.data_type(), DataType::Utf8 | DataType::Int32)
                    )
            }
        }
    }
}

fn contains_map(ty: &DataType) -> bool {
    match ty {
        DataType::Map(..) => true,
        DataType::List(field) => contains_map(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_map(field.data_type())),
        _ => false,
    }
}

fn canonical_temporal_key(semantic: &crate::ValueTypeRef, data_type: &DataType) -> bool {
    if let Some(temporal) = semantic.temporal() {
        return temporal.carrier() == *data_type;
    }
    let child = |index, data_type| {
        semantic.child_type_at(index).is_some_and(|semantic| {
            canonical_key_member(data_type) || canonical_temporal_key(semantic, data_type)
        })
    };
    match data_type {
        DataType::List(field) => child(0, field.data_type()),
        DataType::Struct(fields) => {
            fields.len() == semantic.child_count()
                && fields
                    .iter()
                    .enumerate()
                    .all(|(index, field)| child(index, field.data_type()))
        }
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => fields
                .iter()
                .enumerate()
                .all(|(index, field)| child(index, field.data_type())),
            _ => false,
        },
        _ => false,
    }
}

/// Leaf types the canonical row encoding writes byte-for-byte at any depth.
///
/// `LargeUtf8` is deliberately absent: the encoding reaches it only as a whole
/// column, never as a member of a list, struct or map.
fn canonical_key_leaf(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Timestamp(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::FixedSizeBinary(LARGEINT_BYTE_WIDTH)
    )
}

/// A list, struct or map whose every member the canonical row encoding reaches.
fn canonical_key_nesting(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field) => canonical_key_member(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .all(|field| canonical_key_member(field.data_type())),
        DataType::Map(entries, _) => match entries.data_type() {
            DataType::Struct(fields) => {
                fields.len() == 2
                    && fields
                        .iter()
                        .all(|field| canonical_key_member(field.data_type()))
            }
            _ => false,
        },
        _ => false,
    }
}

/// One member reached through a nesting.
///
/// `Null` is admissible: the encoding writes the same absent-value byte for it
/// as for a null member of any other type.
fn canonical_key_member(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null | DataType::Float32 | DataType::Float64
    ) || canonical_key_leaf(data_type)
        || canonical_key_nesting(data_type)
}

/// Stable identity of one logical partition space.
///
/// Equal hash definitions and counts do not prove co-partitioning: separately
/// produced layouts may share the same shape. Every co-partition boundary must
/// therefore compare this identity along with the full definition.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartitionSpaceId([u8; 32]);

impl PartitionSpaceId {
    pub fn try_new(value: [u8; 32]) -> Result<Self, PartitionSpaceIdentityError> {
        if value == [0; 32] {
            return Err(PartitionSpaceIdentityError::Zero);
        }
        Ok(Self(value))
    }

    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartitionSpaceIdentityError {
    Zero,
}

impl fmt::Display for PartitionSpaceIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("partition-space identity must be non-zero")
    }
}

impl std::error::Error for PartitionSpaceIdentityError {}

/// Stable symbol instantiated to a concrete count by task assignment.
///
/// The final plan carries this identity and its admissible domain, never a live
/// topology count or mutable route.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PartitionCountParameterId([u8; 32]);

impl PartitionCountParameterId {
    pub fn try_new(value: [u8; 32]) -> Result<Self, PartitionCountParameterIdentityError> {
        if value == [0; 32] {
            return Err(PartitionCountParameterIdentityError::Zero);
        }
        Ok(Self(value))
    }

    pub const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartitionCountParameterIdentityError {
    Zero,
}

impl fmt::Display for PartitionCountParameterIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("partition-count parameter identity must be non-zero")
    }
}

impl std::error::Error for PartitionCountParameterIdentityError {}

/// Closed identity of the bucket-to-ordinal layout consumed by execution.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum BucketLayoutAlgorithm {
    /// Every bucket owns exactly one ordinal in `[0, bucket_count)`.
    DenseZeroBasedV1,
}

impl BucketLayoutAlgorithm {
    pub const fn stable_name(self) -> &'static str {
        match self {
            Self::DenseZeroBasedV1 => "novarocks.bucket-layout.dense-zero-based.v1",
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field};

    use super::{
        BucketLayoutAlgorithm, PartitionCountParameterId, PartitionCountParameterIdentityError,
        PartitionHashAlgorithm, PartitionSpaceId, PartitionSpaceIdentityError,
    };

    #[test]
    fn grouping_hash_only_switches_for_a_map_reachable_in_the_key_type() {
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Int64, false),
                    Field::new("value", DataType::Int64, true),
                ]
                .into(),
            ),
            false,
        ));
        let map = DataType::Map(entries, false);
        let nested = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(vec![Field::new("map", map.clone(), true)].into()),
            true,
        )));
        assert_eq!(
            PartitionHashAlgorithm::for_grouping_keys([&DataType::Int64]),
            PartitionHashAlgorithm::NativeExchangeV1,
        );
        assert_eq!(
            PartitionHashAlgorithm::for_grouping_keys([&DataType::Int64, &map]),
            PartitionHashAlgorithm::NativeExchangeV2,
        );
        assert_eq!(
            PartitionHashAlgorithm::for_grouping_keys([&nested]),
            PartitionHashAlgorithm::NativeExchangeV2,
        );
    }

    #[test]
    fn native_exchange_hash_identity_includes_revision_and_null_semantics() {
        let algorithm = PartitionHashAlgorithm::NativeExchangeV1;
        assert_eq!(algorithm.stable_name(), "novarocks.native-exchange.v1");
        assert!(algorithm.nulls_equal());
        assert!(algorithm.supports_partition_key(&DataType::Int64));
        // The exchange folds signed zero and NaN the way grouping does, so a
        // float lands where its group does; a bucket keeps Iceberg's domain,
        // which has no float in it.
        assert!(algorithm.supports_partition_key(&DataType::Float64));
        assert!(algorithm.supports_partition_key(&DataType::Float32));
        assert!(
            !PartitionHashAlgorithm::NativeBucketCrc32V1.supports_partition_key(&DataType::Float64)
        );
    }

    #[test]
    fn native_exchange_reaches_a_nested_key_its_encoding_covers() {
        let exchange = PartitionHashAlgorithm::NativeExchangeV1;
        let bucket = PartitionHashAlgorithm::NativeBucketCrc32V1;
        let list_of = |inner: DataType| DataType::List(Arc::new(Field::new("item", inner, true)));
        let nested = list_of(list_of(DataType::Int64));
        // The exchange encodes a nesting member by member, so the depth is not
        // what decides; the members are. A bucket keeps Iceberg's two shapes.
        assert!(exchange.supports_partition_key(&nested));
        assert!(!bucket.supports_partition_key(&nested));
        assert!(
            exchange.supports_partition_key(&DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Float64, true),
                        ]
                        .into()
                    ),
                    false,
                )),
                false,
            ))
        );
        // LargeUtf8 is a key of its own but is not reachable as a member: the
        // canonical encoding has no case for it below the top level.
        assert!(exchange.supports_partition_key(&DataType::LargeUtf8));
        assert!(!exchange.supports_partition_key(&list_of(DataType::LargeUtf8)));
        assert!(!exchange.supports_partition_key(&list_of(DataType::UInt32)));
    }

    #[test]
    fn bucket_hash_and_layout_have_distinct_closed_identities() {
        assert_eq!(
            PartitionHashAlgorithm::NativeBucketCrc32V1.stable_name(),
            "novarocks.native-bucket-crc32.v1"
        );
        assert_eq!(
            BucketLayoutAlgorithm::DenseZeroBasedV1.stable_name(),
            "novarocks.bucket-layout.dense-zero-based.v1"
        );
        assert_ne!(
            PartitionHashAlgorithm::NativeBucketCrc32V1,
            PartitionHashAlgorithm::NativeExchangeV1
        );
    }

    #[test]
    fn partition_space_identity_rejects_the_absent_identity() {
        assert_eq!(
            PartitionSpaceId::try_new([0; 32]),
            Err(PartitionSpaceIdentityError::Zero)
        );
        assert_eq!(
            PartitionSpaceId::try_new([7; 32]).unwrap().as_bytes(),
            [7; 32]
        );
        assert_eq!(
            PartitionCountParameterId::try_new([0; 32]),
            Err(PartitionCountParameterIdentityError::Zero)
        );
    }
}

#[cfg(test)]
mod hierarchical_hash_tests {
    use super::PartitionHashDomain::{DriverV1, InstanceV1};

    // Independent integer SplitMix64 vectors freeze both domains and wrapping.
    #[test]
    fn hierarchy_v1_domains_have_fixed_full_width_vectors() {
        assert_eq!(InstanceV1.stable_name(), "novarocks.partition.instance.v1");
        assert_eq!(DriverV1.stable_name(), "novarocks.partition.driver.v1");
        for (input, instance, driver) in [
            (0, 0x16b2_7825_bb4c_c82c, 0x8423_7b5b_9fcc_d701),
            (1, 0x06c5_6cd3_8f24_7929, 0x31af_a579_a3e8_bcad),
            (u64::MAX, 0x13ec_1345_992c_6b53, 0x6232_109b_2327_1493),
        ] {
            assert_eq!(InstanceV1.mix(input), instance);
            assert_eq!(DriverV1.mix(input), driver);
        }
    }

    // Distribution is a functional property. The fixed input is independent
    // of writer count, batching and the partition implementation's output.
    #[test]
    fn both_levels_use_all_lanes_for_equal_and_shared_factor_counts() {
        for (instances, drivers) in [(4_usize, 4_usize), (4, 2), (3, 4)] {
            let mut seen = vec![vec![false; drivers]; instances];
            for key in 0_u64..4096 {
                let instance = (InstanceV1.mix(key) % instances as u64) as usize;
                let driver = (DriverV1.mix(key) % drivers as u64) as usize;
                seen[instance][driver] = true;
            }
            assert!(seen.into_iter().flatten().all(|present| present));
        }
    }
}

#[cfg(test)]
mod hierarchical_contract_tests {
    use super::*;

    #[test]
    fn hierarchy_has_its_own_identity_and_the_same_canonical_key_domain() {
        assert_ne!(
            PartitionHashAlgorithm::NativeHierarchicalV1.stable_name(),
            PartitionHashAlgorithm::NativeExchangeV1.stable_name()
        );
        for ty in [
            DataType::Int64,
            DataType::Utf8,
            DataType::Float64,
            DataType::UInt64,
            DataType::Binary,
        ] {
            assert_eq!(
                PartitionHashAlgorithm::NativeHierarchicalV1.supports_partition_key(&ty),
                PartitionHashAlgorithm::NativeExchangeV1.supports_partition_key(&ty)
            );
        }
        assert_ne!(
            PartitionHashDomain::InstanceV1.mix(17),
            PartitionHashDomain::DriverV1.mix(17)
        );
    }
}
