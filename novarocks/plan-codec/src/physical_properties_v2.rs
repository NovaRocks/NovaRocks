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

//! Complete physical-property vocabulary projection. Receiving validates wire
//! shape and sole partition identity constructors. Fragment/package validation
//! remains mandatory for key references/types, count/bucket legality and
//! cross-occurrence partition definitions. These explicit request bounds are
//! not a host MEM grant or an internal allocator cooperation claim.

use crate::allocation_exit_v2::reserve_exit;
use novarocks_physical_plan as physical;
use novarocks_proto_models::{physical_control_v2::Empty, physical_package_v2 as wire};
use novarocks_type_contract::{
    BucketLayoutAlgorithm, CompileCheckpoints, CompileControlError, CompilePhase,
    PartitionCountParameterId, PartitionHashAlgorithm, PartitionSpaceId, PureCompileControl,
};
use std::{alloc::Layout, fmt, mem::size_of};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhysicalPropertyProjectionLimits {
    pub max_value_references: usize,
    pub max_allocation_requests: usize,
    pub max_allocation_request_bytes: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PhysicalPropertyProjectionFacts {
    pub value_reference_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub allocation_request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalPropertyCodecError {
    Control(CompileControlError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for PhysicalPropertyCodecError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl fmt::Display for PhysicalPropertyCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::InvalidShape(text) => f.write_str(text),
        }
    }
}
impl std::error::Error for PhysicalPropertyCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(source) => Some(source),
            _ => None,
        }
    }
}
type Error = PhysicalPropertyCodecError;
fn invalid(text: &'static str) -> Error {
    Error::InvalidShape(text)
}
#[derive(Clone, Copy)]
enum Arithmetic {
    Plain,
    Parent,
}
impl Arithmetic {
    fn error(self, text: &'static str) -> Error {
        match self {
            Self::Plain => invalid(text),
            Self::Parent => CompileControlError::ResourceExhausted.into(),
        }
    }
    fn add(self, a: usize, b: usize) -> Result<usize, Error> {
        a.checked_add(b)
            .ok_or_else(|| self.error("physical property resource sum overflow"))
    }
    fn mul(self, a: usize, b: usize) -> Result<usize, Error> {
        a.checked_mul(b)
            .ok_or_else(|| self.error("physical property resource product overflow"))
    }
    fn bytes<T>(self, count: usize) -> Result<usize, Error> {
        Layout::array::<T>(count)
            .map(|layout| layout.size())
            .map_err(|_| self.error("physical property allocation layout is unrepresentable"))
    }
}
#[cfg(test)]
fn add(a: usize, b: usize) -> Result<usize, Error> {
    Arithmetic::Plain.add(a, b)
}
#[cfg(test)]
fn mul(a: usize, b: usize) -> Result<usize, Error> {
    Arithmetic::Plain.mul(a, b)
}
fn bytes<T>(count: usize) -> Result<usize, Error> {
    Arithmetic::Plain.bytes::<T>(count)
}
fn floor(source: usize, known: usize) -> Result<(), Error> {
    if source < known {
        Err(invalid(
            "physical property source invoice omits original backing",
        ))
    } else {
        Ok(())
    }
}
fn cap(value: usize, limit: usize, work: &mut CompileCheckpoints<'_>) -> Result<(), Error> {
    let admitted = value <= limit;
    work.step()?;
    if admitted {
        Ok(())
    } else {
        Err(invalid("physical property projection envelope exceeded"))
    }
}
fn numerical_facts_core(
    references: usize,
    requests: usize,
    requested_bytes: usize,
    source: usize,
    arithmetic: Arithmetic,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    // All own passes inspect only fixed-size keys/ordering/identity values.
    // This includes numerical gates, constructors, copies and conversion
    // loops. Requested-byte work conservatively covers buffer writes/moves;
    // std allocation and Vec-to-Box internals are bracketed opaque work.
    let result = PhysicalPropertyProjectionFacts {
        value_reference_count: references,
        allocation_requests_upper_bound: requests,
        allocation_request_bytes_upper_bound: requested_bytes,
        coexisting_source_and_request_bytes_upper_bound: arithmetic.add(source, requested_bytes)?,
        cumulative_work_upper_bound: arithmetic.add(
            128,
            arithmetic.add(
                arithmetic.mul(references, 32)?,
                arithmetic.mul(requested_bytes, 4)?,
            )?,
        )?,
    };
    Ok(result)
}
fn fact_axes(
    result: PhysicalPropertyProjectionFacts,
    limits: PhysicalPropertyProjectionLimits,
) -> [(usize, usize); 5] {
    [
        (result.value_reference_count, limits.max_value_references),
        (
            result.allocation_requests_upper_bound,
            limits.max_allocation_requests,
        ),
        (
            result.allocation_request_bytes_upper_bound,
            limits.max_allocation_request_bytes,
        ),
        (
            result.coexisting_source_and_request_bytes_upper_bound,
            limits.max_coexisting_source_and_request_bytes,
        ),
        (result.cumulative_work_upper_bound, limits.max_work),
    ]
}
/// Admit every known property axis before a containing node observes work.
pub(crate) fn check_properties_numerical_facts(
    result: PhysicalPropertyProjectionFacts,
    limits: impl Into<Option<PhysicalPropertyProjectionLimits>>,
) -> Result<(), Error> {
    let Some(limits) = limits.into() else {
        return Ok(());
    };
    for (actual, maximum) in fact_axes(result, limits) {
        if actual > maximum {
            return Err(CompileControlError::ResourceExhausted.into());
        }
    }
    Ok(())
}
fn admit_facts(
    result: PhysicalPropertyProjectionFacts,
    limits: impl Into<Option<PhysicalPropertyProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    let Some(limits) = limits.into() else {
        return Ok(result);
    };
    for (actual, maximum) in fact_axes(result, limits) {
        cap(actual, maximum, work)?;
    }
    Ok(result)
}
fn reserve<T>(count: usize, work: &mut CompileCheckpoints<'_>) -> Result<Vec<T>, Error> {
    bytes::<T>(count)?;
    work.flush()?;
    let mut values = Vec::new();
    let result = values.try_reserve_exact(count);
    reserve_exit::<Error>(result, work)?;
    Ok(values)
}
fn boxed<T>(values: Vec<T>, work: &mut CompileCheckpoints<'_>) -> Result<Box<[T]>, Error> {
    // A shrink request is conservatively admitted even when the standard
    // library reuses the exact Vec allocation. No allocation failure retry.
    work.flush()?;
    let values = values.into_boxed_slice();
    work.flush()?;
    Ok(values)
}
fn copy_bytes(input: &[u8; 32], work: &mut CompileCheckpoints<'_>) -> Result<Vec<u8>, Error> {
    let mut output = reserve(32, work)?;
    for byte in input {
        output.push(*byte);
        work.step()?;
    }
    Ok(output)
}
fn conditional_physical_reference(input: &physical::Distribution) -> usize {
    usize::from(matches!(
        input,
        physical::Distribution::ConditionalKey { .. }
    ))
}
fn conditional_wire_reference(input: &wire::Distribution) -> usize {
    usize::from(matches!(
        input.kind,
        Some(wire::distribution::Kind::ConditionalKey(_))
    ))
}

fn encode_hash(value: PartitionHashAlgorithm) -> i32 {
    match value {
        PartitionHashAlgorithm::NativeExchangeV1 => {
            wire::PartitionHashAlgorithm::NativeExchangeV1 as i32
        }
        PartitionHashAlgorithm::NativeExchangeV2 => {
            wire::PartitionHashAlgorithm::NativeExchangeV2 as i32
        }
        PartitionHashAlgorithm::NativeHierarchicalV1 => {
            wire::PartitionHashAlgorithm::NativeHierarchicalV1 as i32
        }
        PartitionHashAlgorithm::NativeBucketCrc32V1 => {
            wire::PartitionHashAlgorithm::NativeBucketCrc32V1 as i32
        }
    }
}
fn decode_hash(value: i32) -> Result<PartitionHashAlgorithm, Error> {
    match wire::PartitionHashAlgorithm::try_from(value) {
        Ok(wire::PartitionHashAlgorithm::NativeExchangeV1) => {
            Ok(PartitionHashAlgorithm::NativeExchangeV1)
        }
        Ok(wire::PartitionHashAlgorithm::NativeExchangeV2) => {
            Ok(PartitionHashAlgorithm::NativeExchangeV2)
        }
        Ok(wire::PartitionHashAlgorithm::NativeHierarchicalV1) => {
            Ok(PartitionHashAlgorithm::NativeHierarchicalV1)
        }
        Ok(wire::PartitionHashAlgorithm::NativeBucketCrc32V1) => {
            Ok(PartitionHashAlgorithm::NativeBucketCrc32V1)
        }
        Ok(wire::PartitionHashAlgorithm::Unspecified) | Err(_) => Err(invalid(
            "physical property hash algorithm is unknown or unspecified",
        )),
    }
}
pub(crate) fn encode_direction(value: physical::SortDirection) -> i32 {
    match value {
        physical::SortDirection::Ascending => wire::SortDirection::Ascending as i32,
        physical::SortDirection::Descending => wire::SortDirection::Descending as i32,
    }
}
pub(crate) fn decode_direction(value: i32) -> Result<physical::SortDirection, Error> {
    match wire::SortDirection::try_from(value) {
        Ok(wire::SortDirection::Ascending) => Ok(physical::SortDirection::Ascending),
        Ok(wire::SortDirection::Descending) => Ok(physical::SortDirection::Descending),
        Ok(wire::SortDirection::Unspecified) | Err(_) => Err(invalid(
            "physical property sort direction is unknown or unspecified",
        )),
    }
}
pub(crate) fn encode_nulls(value: physical::NullOrdering) -> i32 {
    match value {
        physical::NullOrdering::First => wire::NullOrdering::First as i32,
        physical::NullOrdering::Last => wire::NullOrdering::Last as i32,
    }
}
pub(crate) fn decode_nulls(value: i32) -> Result<physical::NullOrdering, Error> {
    match wire::NullOrdering::try_from(value) {
        Ok(wire::NullOrdering::First) => Ok(physical::NullOrdering::First),
        Ok(wire::NullOrdering::Last) => Ok(physical::NullOrdering::Last),
        Ok(wire::NullOrdering::Unspecified) | Err(_) => Err(invalid(
            "physical property null ordering is unknown or unspecified",
        )),
    }
}
pub(crate) fn encode_multiplicity(value: physical::RowMultiplicity) -> i32 {
    match value {
        physical::RowMultiplicity::SingleCopy => wire::RowMultiplicity::SingleCopy as i32,
        physical::RowMultiplicity::Replicated => wire::RowMultiplicity::Replicated as i32,
    }
}
pub(crate) fn decode_multiplicity(value: i32) -> Result<physical::RowMultiplicity, Error> {
    match wire::RowMultiplicity::try_from(value) {
        Ok(wire::RowMultiplicity::SingleCopy) => Ok(physical::RowMultiplicity::SingleCopy),
        Ok(wire::RowMultiplicity::Replicated) => Ok(physical::RowMultiplicity::Replicated),
        Ok(wire::RowMultiplicity::Unspecified) | Err(_) => Err(invalid(
            "physical property row multiplicity is unknown or unspecified",
        )),
    }
}
fn exact32(input: &[u8]) -> Result<[u8; 32], Error> {
    input
        .try_into()
        .map_err(|_| invalid("physical property identity or digest is not exactly 32 bytes"))
}

/// Allocation-free same-author admission for composing a larger namespace.
/// The caller meters this pass and any later emission separately.
pub(crate) fn preflight_encode_observed(
    input: &physical::PhysicalProperties,
    source: usize,
    limits: impl Into<Option<PhysicalPropertyProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    admit_facts(
        properties_encode_numerical_facts(input, source)?,
        limits,
        work,
    )
}

/// The original complete property counts, before a containing node observes work.
pub(crate) fn properties_encode_numerical_facts(
    input: &physical::PhysicalProperties,
    source: usize,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    properties_encode_numerical_facts_core(input, source, Arithmetic::Plain)
}
pub(crate) fn properties_encode_numerical_facts_in(
    input: &physical::PhysicalProperties,
    source: usize,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    properties_encode_numerical_facts_core(input, source, Arithmetic::Parent)
}
fn properties_encode_numerical_facts_core(
    input: &physical::PhysicalProperties,
    source: usize,
    arithmetic: Arithmetic,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    let keys = match &input.distribution {
        physical::Distribution::Hash { keys, .. }
        | physical::Distribution::ConditionalKey { keys, .. }
        | physical::Distribution::BucketShuffle { keys, .. } => &**keys,
        physical::Distribution::Unconstrained
        | physical::Distribution::Singleton
        | physical::Distribution::RoundRobin
        | physical::Distribution::Broadcast => &[],
    };
    let keyed = matches!(
        input.distribution,
        physical::Distribution::Hash { .. }
            | physical::Distribution::ConditionalKey { .. }
            | physical::Distribution::BucketShuffle { .. }
    );
    let references = arithmetic.add(
        arithmetic.add(
            keys.len(),
            conditional_physical_reference(&input.distribution),
        )?,
        input.ordering.len(),
    )?;
    floor(
        source,
        arithmetic.add(
            size_of::<physical::PhysicalProperties>(),
            arithmetic.add(
                arithmetic.bytes::<physical::ValueId>(keys.len())?,
                arithmetic.bytes::<physical::OrderingKey>(input.ordering.len())?,
            )?,
        )?,
    )?;
    let requested = arithmetic.add(
        arithmetic.add(
            arithmetic.bytes::<u32>(keys.len())?,
            arithmetic.bytes::<wire::OrderingKey>(input.ordering.len())?,
        )?,
        if keyed { 64 } else { 0 },
    )?;
    let requests = arithmetic.add(
        usize::from(!keys.is_empty()),
        arithmetic.add(
            usize::from(!input.ordering.is_empty()),
            if keyed { 2 } else { 0 },
        )?,
    )?;
    numerical_facts_core(references, requests, requested, source, arithmetic)
}

/// Emit only after the containing owner has admitted the original request.
/// Both standalone and full-property paths use this exact distribution grammar.
pub(crate) fn emit_distribution_observed(
    input: &physical::Distribution,
    work: &mut CompileCheckpoints<'_>,
) -> Result<wire::Distribution, Error> {
    let keys = match input {
        physical::Distribution::Hash { keys, .. }
        | physical::Distribution::ConditionalKey { keys, .. }
        | physical::Distribution::BucketShuffle { keys, .. } => &**keys,
        _ => &[],
    };
    let mut output_keys = reserve(keys.len(), work)?;
    for key in keys {
        output_keys.push(key.get());
        work.step()?;
    }
    let kind = match input {
        physical::Distribution::Unconstrained => wire::distribution::Kind::Unconstrained(Empty {}),
        physical::Distribution::Singleton => wire::distribution::Kind::Singleton(Empty {}),
        physical::Distribution::RoundRobin => wire::distribution::Kind::RoundRobin(Empty {}),
        physical::Distribution::Broadcast => wire::distribution::Kind::Broadcast(Empty {}),
        physical::Distribution::Hash { scheme, .. } => {
            wire::distribution::Kind::Hash(wire::HashDistribution {
                key_value_ids: output_keys,
                scheme: Some(wire::HashPartitionScheme {
                    partition_space: copy_bytes(&scheme.space.as_bytes(), work)?,
                    count: Some(wire::PartitionCountParameter {
                        id: copy_bytes(&scheme.count.id.as_bytes(), work)?,
                        admissible: Some(wire::PartitionCountDomain {
                            min: scheme.count.admissible.min,
                            max: scheme.count.admissible.max,
                            requires_power_of_two: scheme.count.admissible.requires_power_of_two,
                        }),
                    }),
                    algorithm: encode_hash(scheme.definition.algorithm),
                }),
            })
        }
        physical::Distribution::ConditionalKey {
            applies, scheme, ..
        } => wire::distribution::Kind::ConditionalKey(wire::ConditionalKeyDistribution {
            applies_value_id: Some(applies.get()),
            key_value_ids: output_keys,
            scheme: Some(wire::HashPartitionScheme {
                partition_space: copy_bytes(&scheme.space.as_bytes(), work)?,
                count: Some(wire::PartitionCountParameter {
                    id: copy_bytes(&scheme.count.id.as_bytes(), work)?,
                    admissible: Some(wire::PartitionCountDomain {
                        min: scheme.count.admissible.min,
                        max: scheme.count.admissible.max,
                        requires_power_of_two: scheme.count.admissible.requires_power_of_two,
                    }),
                }),
                algorithm: encode_hash(scheme.definition.algorithm),
            }),
        }),
        physical::Distribution::BucketShuffle { scheme, .. } => {
            wire::distribution::Kind::BucketShuffle(wire::BucketDistribution {
                key_value_ids: output_keys,
                scheme: Some(wire::BucketPartitionScheme {
                    partition_space: copy_bytes(&scheme.space.as_bytes(), work)?,
                    bucket_count: scheme.bucket_count,
                    hash: encode_hash(scheme.hash),
                    layout: match scheme.layout {
                        BucketLayoutAlgorithm::DenseZeroBasedV1 => {
                            wire::BucketLayoutAlgorithm::DenseZeroBasedV1 as i32
                        }
                    },
                    ordinal_domain: Some(wire::BucketOrdinalDomainProof {
                        first_ordinal: scheme.ordinal_domain.first_ordinal,
                        ordinal_count: scheme.ordinal_domain.ordinal_count,
                        evidence_digest: copy_bytes(&scheme.ordinal_domain.evidence_digest, work)?,
                    }),
                }),
            })
        }
    };
    work.step()?;
    Ok(wire::Distribution { kind: Some(kind) })
}

pub(crate) fn encode_observed(
    input: &physical::PhysicalProperties,
    source: usize,
    limits: impl Into<Option<PhysicalPropertyProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::PhysicalProperties, PhysicalPropertyProjectionFacts), Error> {
    let facts = preflight_encode_observed(input, source, limits, work)?;
    let distribution = emit_distribution_observed(&input.distribution, work)?;
    let mut ordering = reserve(input.ordering.len(), work)?;
    for key in &input.ordering {
        ordering.push(wire::OrderingKey {
            value_id: Some(key.value.get()),
            direction: encode_direction(key.direction),
            null_ordering: encode_nulls(key.null_ordering),
        });
        work.step()?;
    }
    Ok((
        wire::PhysicalProperties {
            distribution: Some(distribution),
            row_multiplicity: encode_multiplicity(input.row_multiplicity),
            ordering,
        },
        facts,
    ))
}

// A no-heap validated header. It is never a public receiving semantic owner;
// the complete output typed value is constructed in this same call.
enum DistributionHeader {
    Unconstrained,
    Singleton,
    RoundRobin,
    Broadcast,
    Hash(physical::HashPartitionScheme),
    ConditionalKey(physical::ValueId, physical::HashPartitionScheme),
    Bucket(physical::BucketPartitionScheme),
}
fn decode_header(
    input: &wire::distribution::Kind,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DistributionHeader, Error> {
    let header = match input {
        wire::distribution::Kind::Unconstrained(_) => DistributionHeader::Unconstrained,
        wire::distribution::Kind::Singleton(_) => DistributionHeader::Singleton,
        wire::distribution::Kind::RoundRobin(_) => DistributionHeader::RoundRobin,
        wire::distribution::Kind::Broadcast(_) => DistributionHeader::Broadcast,
        wire::distribution::Kind::Hash(hash) => {
            let scheme = hash
                .scheme
                .as_ref()
                .ok_or_else(|| invalid("physical property hash scheme is absent"))?;
            let count = scheme
                .count
                .as_ref()
                .ok_or_else(|| invalid("physical property count parameter is absent"))?;
            let domain = count
                .admissible
                .as_ref()
                .ok_or_else(|| invalid("physical property count domain is absent"))?;
            DistributionHeader::Hash(physical::HashPartitionScheme {
                space: PartitionSpaceId::try_new(exact32(&scheme.partition_space)?)
                    .map_err(|_| invalid("physical property partition space is zero"))?,
                count: physical::PartitionCountParameter {
                    id: PartitionCountParameterId::try_new(exact32(&count.id)?).map_err(|_| {
                        invalid("physical property count parameter identity is zero")
                    })?,
                    admissible: physical::PartitionCountDomain {
                        min: domain.min,
                        max: domain.max,
                        requires_power_of_two: domain.requires_power_of_two,
                    },
                },
                definition: physical::HashDefinition {
                    algorithm: decode_hash(scheme.algorithm)?,
                },
            })
        }
        wire::distribution::Kind::ConditionalKey(hash) => {
            let scheme = hash
                .scheme
                .as_ref()
                .ok_or_else(|| invalid("physical property hash scheme is absent"))?;
            let count = scheme
                .count
                .as_ref()
                .ok_or_else(|| invalid("physical property count parameter is absent"))?;
            let domain = count
                .admissible
                .as_ref()
                .ok_or_else(|| invalid("physical property count domain is absent"))?;
            DistributionHeader::ConditionalKey(
                physical::ValueId::new(
                    hash.applies_value_id
                        .ok_or_else(|| invalid("conditional partition applicability is absent"))?,
                ),
                physical::HashPartitionScheme {
                    space: PartitionSpaceId::try_new(exact32(&scheme.partition_space)?)
                        .map_err(|_| invalid("physical property partition space is zero"))?,
                    count: physical::PartitionCountParameter {
                        id: PartitionCountParameterId::try_new(exact32(&count.id)?).map_err(
                            |_| invalid("physical property count parameter identity is zero"),
                        )?,
                        admissible: physical::PartitionCountDomain {
                            min: domain.min,
                            max: domain.max,
                            requires_power_of_two: domain.requires_power_of_two,
                        },
                    },
                    definition: physical::HashDefinition {
                        algorithm: decode_hash(scheme.algorithm)?,
                    },
                },
            )
        }
        wire::distribution::Kind::BucketShuffle(bucket) => {
            let scheme = bucket
                .scheme
                .as_ref()
                .ok_or_else(|| invalid("physical property bucket scheme is absent"))?;
            let proof = scheme
                .ordinal_domain
                .as_ref()
                .ok_or_else(|| invalid("physical property bucket ordinal proof is absent"))?;
            let layout = match wire::BucketLayoutAlgorithm::try_from(scheme.layout) {
                Ok(wire::BucketLayoutAlgorithm::DenseZeroBasedV1) => {
                    BucketLayoutAlgorithm::DenseZeroBasedV1
                }
                Ok(wire::BucketLayoutAlgorithm::Unspecified) | Err(_) => {
                    return Err(invalid(
                        "physical property bucket layout is unknown or unspecified",
                    ));
                }
            };
            DistributionHeader::Bucket(physical::BucketPartitionScheme {
                space: PartitionSpaceId::try_new(exact32(&scheme.partition_space)?)
                    .map_err(|_| invalid("physical property partition space is zero"))?,
                bucket_count: scheme.bucket_count,
                hash: decode_hash(scheme.hash)?,
                layout,
                ordinal_domain: physical::BucketOrdinalDomainProof {
                    first_ordinal: proof.first_ordinal,
                    ordinal_count: proof.ordinal_count,
                    evidence_digest: exact32(&proof.evidence_digest)?,
                },
            })
        }
    };
    work.step()?;
    Ok(header)
}
/// Allocation-free resource admission through the sole wire-property grammar.
/// Exact header/ordering validation still occurs before decoder reservations.
fn decode_distribution_source(input: &wire::Distribution) -> Result<(&[u32], usize), Error> {
    decode_distribution_source_core(input, Arithmetic::Plain)
}
fn decode_distribution_source_core(
    input: &wire::Distribution,
    arithmetic: Arithmetic,
) -> Result<(&[u32], usize), Error> {
    let kind = input
        .kind
        .as_ref()
        .ok_or_else(|| invalid("physical property distribution kind is absent"))?;
    let source = match kind {
        wire::distribution::Kind::Hash(hash) => {
            let scheme = hash
                .scheme
                .as_ref()
                .ok_or_else(|| invalid("physical property hash scheme is absent"))?;
            let count = scheme
                .count
                .as_ref()
                .ok_or_else(|| invalid("physical property count parameter is absent"))?;
            (
                &hash.key_value_ids[..],
                arithmetic.add(
                    arithmetic.bytes::<u32>(hash.key_value_ids.capacity())?,
                    arithmetic.add(scheme.partition_space.capacity(), count.id.capacity())?,
                )?,
            )
        }
        wire::distribution::Kind::ConditionalKey(hash) => {
            let scheme = hash
                .scheme
                .as_ref()
                .ok_or_else(|| invalid("physical property hash scheme is absent"))?;
            let count = scheme
                .count
                .as_ref()
                .ok_or_else(|| invalid("physical property count parameter is absent"))?;
            (
                &hash.key_value_ids[..],
                arithmetic.add(
                    arithmetic.bytes::<u32>(hash.key_value_ids.capacity())?,
                    arithmetic.add(scheme.partition_space.capacity(), count.id.capacity())?,
                )?,
            )
        }
        wire::distribution::Kind::BucketShuffle(bucket) => {
            let scheme = bucket
                .scheme
                .as_ref()
                .ok_or_else(|| invalid("physical property bucket scheme is absent"))?;
            let proof = scheme
                .ordinal_domain
                .as_ref()
                .ok_or_else(|| invalid("physical property bucket ordinal proof is absent"))?;
            (
                &bucket.key_value_ids[..],
                arithmetic.add(
                    arithmetic.bytes::<u32>(bucket.key_value_ids.capacity())?,
                    arithmetic.add(
                        scheme.partition_space.capacity(),
                        proof.evidence_digest.capacity(),
                    )?,
                )?,
            )
        }
        wire::distribution::Kind::Unconstrained(_)
        | wire::distribution::Kind::Singleton(_)
        | wire::distribution::Kind::RoundRobin(_)
        | wire::distribution::Kind::Broadcast(_) => (&[][..], 0),
    };
    Ok(source)
}

fn decode_source(input: &wire::PhysicalProperties) -> Result<(&[u32], usize), Error> {
    decode_source_core(input, Arithmetic::Plain)
}
fn decode_source_core(
    input: &wire::PhysicalProperties,
    arithmetic: Arithmetic,
) -> Result<(&[u32], usize), Error> {
    let distribution = input
        .distribution
        .as_ref()
        .ok_or_else(|| invalid("physical property distribution is absent"))?;
    decode_distribution_source_core(distribution, arithmetic)
}

pub(crate) fn preflight_decode_observed(
    input: &wire::PhysicalProperties,
    source: usize,
    limits: impl Into<Option<PhysicalPropertyProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    admit_facts(
        properties_decode_numerical_facts(input, source)?,
        limits,
        work,
    )
}

/// The original complete property counts, before a containing node observes work.
pub(crate) fn properties_decode_numerical_facts(
    input: &wire::PhysicalProperties,
    source: usize,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    properties_decode_numerical_facts_core(input, source, Arithmetic::Plain)
}
pub(crate) fn properties_decode_numerical_facts_in(
    input: &wire::PhysicalProperties,
    source: usize,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    properties_decode_numerical_facts_core(input, source, Arithmetic::Parent)
}
fn properties_decode_numerical_facts_core(
    input: &wire::PhysicalProperties,
    source: usize,
    arithmetic: Arithmetic,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    let (keys, extra_source) = decode_source_core(input, arithmetic)?;
    floor(
        source,
        arithmetic.add(
            size_of::<wire::PhysicalProperties>(),
            arithmetic.add(
                extra_source,
                arithmetic.bytes::<wire::OrderingKey>(input.ordering.capacity())?,
            )?,
        )?,
    )?;
    // Vec-to-Box may shrink to the exact length; admit both requested layouts
    // and their coexistence even when the library actually reuses backing.
    let requested = arithmetic.mul(
        arithmetic.add(
            arithmetic.bytes::<physical::ValueId>(keys.len())?,
            arithmetic.bytes::<physical::OrderingKey>(input.ordering.len())?,
        )?,
        2,
    )?;
    let requests = arithmetic.mul(
        arithmetic.add(
            usize::from(!keys.is_empty()),
            usize::from(!input.ordering.is_empty()),
        )?,
        2,
    )?;
    numerical_facts_core(
        arithmetic.add(
            arithmetic.add(
                keys.len(),
                conditional_wire_reference(
                    input
                        .distribution
                        .as_ref()
                        .ok_or_else(|| invalid("physical property distribution is absent"))?,
                ),
            )?,
            input.ordering.len(),
        )?,
        requests,
        requested,
        source,
        arithmetic,
    )
}

fn materialize_distribution_observed(
    keys: &[u32],
    header: DistributionHeader,
    work: &mut CompileCheckpoints<'_>,
) -> Result<physical::Distribution, Error> {
    let mut output_keys = reserve(keys.len(), work)?;
    for key in keys {
        output_keys.push(physical::ValueId::new(*key));
        work.step()?;
    }
    let output_keys = boxed(output_keys, work)?;
    let distribution = match header {
        DistributionHeader::Unconstrained => physical::Distribution::Unconstrained,
        DistributionHeader::Singleton => physical::Distribution::Singleton,
        DistributionHeader::RoundRobin => physical::Distribution::RoundRobin,
        DistributionHeader::Broadcast => physical::Distribution::Broadcast,
        DistributionHeader::Hash(scheme) => physical::Distribution::Hash {
            keys: output_keys,
            scheme,
        },
        DistributionHeader::ConditionalKey(applies, scheme) => {
            physical::Distribution::ConditionalKey {
                applies,
                keys: output_keys,
                scheme,
            }
        }
        DistributionHeader::Bucket(scheme) => physical::Distribution::BucketShuffle {
            keys: output_keys,
            scheme,
        },
    };
    work.step()?;
    Ok(distribution)
}

type DecodedPropertyHeader<'a> = (&'a [u32], DistributionHeader, physical::RowMultiplicity);
fn decode_property_header_observed<'a>(
    input: &'a wire::PhysicalProperties,
    work: &mut CompileCheckpoints<'_>,
) -> Result<DecodedPropertyHeader<'a>, Error> {
    let kind = input
        .distribution
        .as_ref()
        .and_then(|distribution| distribution.kind.as_ref())
        .ok_or_else(|| invalid("prepared physical property distribution kind is absent"))?;
    let (keys, _) = decode_source(input)?;
    let row_multiplicity = decode_multiplicity(input.row_multiplicity)?;
    let header = decode_header(kind, work)?;
    // Validate the complete ordering grammar before the first reserve; the
    // later conversion only consumes this immutable borrowed snapshot.
    for key in &input.ordering {
        key.value_id
            .ok_or_else(|| invalid("physical property ordering value is absent"))?;
        decode_direction(key.direction)?;
        decode_nulls(key.null_ordering)?;
        work.step()?;
    }
    Ok((keys, header, row_multiplicity))
}

/// Validate the original closed header and ordering before node reservations.
/// This borrows the decoder's sole grammar and performs no materialization.
pub(crate) fn validate_properties_source_observed(
    input: &wire::PhysicalProperties,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    decode_property_header_observed(input, work).map(|_| ())
}

pub(crate) fn decode_observed(
    input: &wire::PhysicalProperties,
    source: usize,
    limits: impl Into<Option<PhysicalPropertyProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    (
        physical::PhysicalProperties,
        PhysicalPropertyProjectionFacts,
    ),
    Error,
> {
    let facts = preflight_decode_observed(input, source, limits, work)?;
    let (keys, header, row_multiplicity) = decode_property_header_observed(input, work)?;
    let distribution = materialize_distribution_observed(keys, header, work)?;
    let mut ordering = reserve(input.ordering.len(), work)?;
    for key in &input.ordering {
        ordering.push(physical::OrderingKey {
            value: physical::ValueId::new(
                key.value_id
                    .ok_or_else(|| invalid("prepared property ordering value is absent"))?,
            ),
            direction: decode_direction(key.direction)?,
            null_ordering: decode_nulls(key.null_ordering)?,
        });
        work.step()?;
    }
    let ordering = boxed(ordering, work)?;
    Ok((
        physical::PhysicalProperties {
            distribution,
            row_multiplicity,
            ordering,
        },
        facts,
    ))
}
fn finish<T>(work: CompileCheckpoints<'_>, result: Result<T, Error>) -> Result<T, Error> {
    if matches!(&result, Err(Error::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
/// Allocation-free distribution admission for an original containing source.
/// No ordering or multiplicity facts are manufactured for a writer requirement.
pub(crate) fn preflight_distribution_encode_observed(
    input: &physical::Distribution,
    source: usize,
    limits: impl Into<Option<PhysicalPropertyProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    admit_facts(
        distribution_encode_numerical_facts(input, source)?,
        limits,
        work,
    )
}

fn distribution_encode_source(input: &physical::Distribution) -> (&[physical::ValueId], bool) {
    match input {
        physical::Distribution::Hash { keys, .. }
        | physical::Distribution::ConditionalKey { keys, .. }
        | physical::Distribution::BucketShuffle { keys, .. } => (&**keys, true),
        physical::Distribution::Unconstrained
        | physical::Distribution::Singleton
        | physical::Distribution::RoundRobin
        | physical::Distribution::Broadcast => (&[][..], false),
    }
}

/// Necessary backing only; the containing source already owns the inline header.
pub(crate) fn distribution_encode_source_backing_floor(
    input: &physical::Distribution,
) -> Result<usize, Error> {
    bytes::<physical::ValueId>(distribution_encode_source(input).0.len())
}

/// Reuse the original presence/capacity author without a second distribution grammar.
pub(crate) fn distribution_decode_source_backing_floor(
    input: &wire::Distribution,
) -> Result<usize, Error> {
    Ok(decode_distribution_source(input)?.1)
}

/// Original numerical author for a containing node's synchronous admission.
/// No observation, allocation, deep traversal or second source invoice occurs.
pub(crate) fn distribution_encode_numerical_facts(
    input: &physical::Distribution,
    source: usize,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    distribution_encode_numerical_facts_core(input, source, Arithmetic::Plain)
}
pub(crate) fn distribution_encode_numerical_facts_in(
    input: &physical::Distribution,
    source: usize,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    distribution_encode_numerical_facts_core(input, source, Arithmetic::Parent)
}
fn distribution_encode_numerical_facts_core(
    input: &physical::Distribution,
    source: usize,
    arithmetic: Arithmetic,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    let (keys, keyed) = distribution_encode_source(input);
    floor(
        source,
        arithmetic.add(
            size_of::<physical::Distribution>(),
            arithmetic.bytes::<physical::ValueId>(keys.len())?,
        )?,
    )?;
    numerical_facts_core(
        arithmetic.add(keys.len(), conditional_physical_reference(input))?,
        arithmetic.add(usize::from(!keys.is_empty()), if keyed { 2 } else { 0 })?,
        arithmetic.add(
            arithmetic.bytes::<u32>(keys.len())?,
            if keyed { 64 } else { 0 },
        )?,
        source,
        arithmetic,
    )
}

pub(crate) fn preflight_distribution_decode_observed(
    input: &wire::Distribution,
    source: usize,
    limits: impl Into<Option<PhysicalPropertyProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    admit_facts(
        distribution_decode_numerical_facts(input, source)?,
        limits,
        work,
    )
}

/// The same raw-capacity/header-presence author, before any child observation.
pub(crate) fn distribution_decode_numerical_facts(
    input: &wire::Distribution,
    source: usize,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    distribution_decode_numerical_facts_core(input, source, Arithmetic::Plain)
}
pub(crate) fn distribution_decode_numerical_facts_in(
    input: &wire::Distribution,
    source: usize,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    distribution_decode_numerical_facts_core(input, source, Arithmetic::Parent)
}
fn distribution_decode_numerical_facts_core(
    input: &wire::Distribution,
    source: usize,
    arithmetic: Arithmetic,
) -> Result<PhysicalPropertyProjectionFacts, Error> {
    let (keys, extra_source) = decode_distribution_source_core(input, arithmetic)?;
    floor(
        source,
        arithmetic.add(size_of::<wire::Distribution>(), extra_source)?,
    )?;
    numerical_facts_core(
        arithmetic.add(keys.len(), conditional_wire_reference(input))?,
        arithmetic.mul(usize::from(!keys.is_empty()), 2)?,
        arithmetic.mul(arithmetic.bytes::<physical::ValueId>(keys.len())?, 2)?,
        source,
        arithmetic,
    )
}

pub(crate) fn encode_distribution_observed(
    input: &physical::Distribution,
    source: usize,
    limits: impl Into<Option<PhysicalPropertyProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(wire::Distribution, PhysicalPropertyProjectionFacts), Error> {
    let facts = preflight_distribution_encode_observed(input, source, limits, work)?;
    Ok((emit_distribution_observed(input, work)?, facts))
}

pub(crate) fn decode_distribution_observed(
    input: &wire::Distribution,
    source: usize,
    limits: impl Into<Option<PhysicalPropertyProjectionLimits>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(physical::Distribution, PhysicalPropertyProjectionFacts), Error> {
    let facts = preflight_distribution_decode_observed(input, source, limits, work)?;
    Ok((
        materialize_distribution_source_observed(input, work)?,
        facts,
    ))
}

/// Check the sole immutable distribution header before containing reservations.
pub(crate) fn validate_distribution_source_observed(
    input: &wire::Distribution,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), Error> {
    let kind = input
        .kind
        .as_ref()
        .ok_or_else(|| invalid("physical property distribution kind is absent"))?;
    decode_header(kind, work).map(|_| ())
}

/// Materialize only after the containing owner has admitted the original source.
/// Keep header validation and key conversion in the standalone author's order.
pub(crate) fn materialize_distribution_source_observed(
    input: &wire::Distribution,
    work: &mut CompileCheckpoints<'_>,
) -> Result<physical::Distribution, Error> {
    let kind = input
        .kind
        .as_ref()
        .ok_or_else(|| invalid("physical property distribution kind is absent"))?;
    let header = decode_header(kind, work)?;
    let (keys, _) = decode_distribution_source(input)?;
    materialize_distribution_observed(keys, header, work)
}

/// Project an independent distribution with explicit source/request bounds.
/// Actual keys, count/bucket domains and partition coherence still require
/// FragmentPackage validation; numerical admission is not a formal MEM grant.
pub fn encode_distribution(
    input: &physical::Distribution,
    source_retained_bytes: usize,
    limits: PhysicalPropertyProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<(wire::Distribution, PhysicalPropertyProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_distribution_observed(input, source_retained_bytes, limits, &mut work);
    finish(work, result)
}

/// Decode only the original distribution; no physical-property defaults or
/// second distribution model are used. Sole partition identity constructors
/// run before the first reserve, including on noncanonical protobuf values.
pub fn decode_distribution(
    input: &wire::Distribution,
    source_retained_bytes: usize,
    limits: PhysicalPropertyProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<(physical::Distribution, PhysicalPropertyProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode_distribution_observed(input, source_retained_bytes, limits, &mut work);
    finish(work, result)
}

/// Encode all property variants without changing occurrence order or identity.
pub fn encode_physical_properties(
    input: &physical::PhysicalProperties,
    source_retained_bytes: usize,
    limits: PhysicalPropertyProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<(wire::PhysicalProperties, PhysicalPropertyProjectionFacts), Error> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode_observed(input, source_retained_bytes, limits, &mut work);
    finish(work, result)
}
/// Decode the complete closed vocabulary. Actual Fragment/FragmentPackage
/// validation must subsequently check property semantics and references.
pub fn decode_physical_properties(
    input: &wire::PhysicalProperties,
    source_retained_bytes: usize,
    limits: PhysicalPropertyProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<
    (
        physical::PhysicalProperties,
        PhysicalPropertyProjectionFacts,
    ),
    Error,
> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = decode_observed(input, source_retained_bytes, limits, &mut work);
    finish(work, result)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod distribution_tests;
