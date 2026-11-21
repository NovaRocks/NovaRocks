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

//! Bitmap payload validity and its own finite wire-count representation.
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
};
use std::{error::Error, fmt, num::NonZeroU16};

#[derive(Debug)]
pub enum BitmapValueFailure {
    InvalidEncoding(BoundedDetail),
    EntryCount { entries: usize },
    StorageExtent { required: u128 },
    MissingSingleton,
    MissingBucket,
    DecodeIo(std::io::Error),
    EncodeIo(std::io::Error),
    Width(std::array::TryFromSliceError),
    Position(std::num::TryFromIntError),
}
impl fmt::Display for BitmapValueFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidEncoding(detail) => detail.fmt(f),
            Self::MissingBucket => f.write_str("bitmap external encode missing 32-bit bucket"),
            Self::DecodeIo(source) => write!(f, "bitmap decode roaring32 payload failed: {source}"),
            Self::EncodeIo(source) => write!(f, "bitmap encode roaring32 payload failed: {source}"),
            Self::Width(source) => write!(f, "bitmap frozen scalar byte width: {source}"),
            Self::Position(source) => {
                write!(f, "bitmap decoder position is not addressable: {source}")
            }
            Self::MissingSingleton => f.write_str("aggregate bitmap singleton has no actual value"),
            Self::StorageExtent { required } => write!(
                f,
                "aggregate bitmap buffer requires {required} bytes beyond its Vec extent"
            ),
            Self::EntryCount { entries } => {
                write!(f, "bitmap internal value count overflow: {entries}")
            }
        }
    }
}
impl Error for BitmapValueFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::DecodeIo(source) | Self::EncodeIo(source) => Some(source),
            Self::Width(source) => Some(source),
            Self::Position(source) => Some(source),
            _ => None,
        }
    }
}
impl BitmapValueFailure {
    pub fn is_data(&self) -> bool {
        matches!(self, Self::InvalidEncoding(_))
            || matches!(self, Self::DecodeIo(source) if matches!(source.kind(), std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::InvalidData | std::io::ErrorKind::InvalidInput))
    }
}
/// The bitmap author alone separates malformed values from outer execution failures.
#[derive(Debug)]
pub enum ClassifiedBitmapFailure {
    Value(BitmapDataFailure),
    Outer(BitmapOuterFailure),
}
#[derive(Debug)]
/// ```compile_fail
/// fn invalid(cause: novarocks_functions::bitmap_value::BitmapDataFailure) {
///     let _ = novarocks_functions::KernelFailure::from_domain(cause);
/// }
/// ```
pub struct BitmapDataFailure(BitmapValueFailure);
#[derive(Debug)]
pub struct BitmapOuterFailure(BitmapValueFailure);
impl BitmapValueFailure {
    pub fn classify(self) -> ClassifiedBitmapFailure {
        if self.is_data() {
            ClassifiedBitmapFailure::Value(BitmapDataFailure(self))
        } else {
            ClassifiedBitmapFailure::Outer(BitmapOuterFailure(self))
        }
    }
}
impl fmt::Display for BitmapDataFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl Error for BitmapDataFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
impl FailureProjection for BitmapDataFailure {
    fn failure_fact(&self) -> FailureFact {
        self.0.failure_fact()
    }
}
impl fmt::Display for BitmapOuterFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl Error for BitmapOuterFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
impl FailureProjection for BitmapOuterFailure {
    fn failure_fact(&self) -> FailureFact {
        self.0.failure_fact()
    }
}
impl novarocks_failure_contract::OuterFailureProjection for BitmapOuterFailure {}

impl FailureProjection for BitmapValueFailure {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::InvalidEncoding(_) => crate::DataFailureReason::InvalidEncoding.failure_fact(),
            Self::DecodeIo(_) if self.is_data() => {
                crate::DataFailureReason::InvalidEncoding.failure_fact()
            }
            Self::DecodeIo(source) | Self::EncodeIo(source) => FailureFact::new(
                if source.kind() == std::io::ErrorKind::OutOfMemory {
                    FailureClass::Resource
                } else {
                    FailureClass::Unexpected
                },
                FailureReason::new(
                    domain::KERNEL,
                    NonZeroU16::new(if source.kind() == std::io::ErrorKind::OutOfMemory {
                        248
                    } else {
                        249
                    })
                    .unwrap(),
                ),
            ),
            Self::Width(_) | Self::Position(_) | Self::MissingBucket => FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(250).unwrap()),
            ),
            Self::MissingSingleton => FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(197).unwrap()),
            ),
            Self::StorageExtent { required } => {
                crate::allocation_failure::rust_array_backing_failure_fact(
                    FailureReason::new(domain::KERNEL, NonZeroU16::new(195).unwrap()),
                    *required,
                    RepresentationBoundary::MinimumUnit,
                )
            }
            Self::EntryCount { entries } => FailureFact::new(
                FailureClass::Representation,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(83).unwrap()),
            )
            .with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::MinimumUnit,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: NonZeroU16::new(2).unwrap(),
                },
                required: u64::try_from(*entries).ok(),
                extent_limit: Some(u64::from(u32::MAX)),
                representable_prefix: None,
            }),
        }
    }
}

pub(super) fn bitmap_wire_count(entries: usize) -> Result<u32, BitmapValueFailure> {
    u32::try_from(entries).map_err(|_| BitmapValueFailure::EntryCount { entries })
}

/// An unobserved caller receives the same explicit data tag the decoder authors.
/// It cannot reclassify callbacks or infer a category from a diagnostic.
pub(crate) struct TypedBitmapPort;
impl super::BitmapDecodePort for TypedBitmapPort {
    type Error = BitmapValueFailure;
    fn failure(&mut self, source: BitmapValueFailure) -> Self::Error {
        source
    }
    fn is_data(error: &Self::Error) -> bool {
        error.is_data()
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn boundary(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
    fn before_render(&mut self, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn before_tree_insert(&mut self, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn before_tree_collection(&mut self, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn before_roaring(&mut self, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn before_u32_collection(&mut self, _: u64) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl super::BitmapAggregateEncodePort for TypedBitmapPort {
    fn before_aggregate_buffer(&mut self, _: usize) -> Result<(), Self::Error> {
        Ok(())
    }
    fn before_aggregate_singleton(&mut self, _: u64) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Default)]
pub struct BitmapOutputTally {
    total: u128,
    largest: u128,
    text: bool,
}
#[derive(Debug)]
struct BitmapOutputExtent {
    total: u128,
    largest: u128,
    text: bool,
}
impl fmt::Display for BitmapOutputExtent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "bitmap {} output extent {} (largest payload {}) exceeds Int32 offsets",
            if self.text { "Utf8" } else { "Binary" },
            self.total,
            self.largest
        )
    }
}
impl Error for BitmapOutputExtent {}
impl FailureProjection for BitmapOutputExtent {
    fn failure_fact(&self) -> FailureFact {
        let minimum = self.largest > i32::MAX as u128;
        FailureFact::new(
            FailureClass::Representation,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(196).unwrap()),
        )
        .with_representation(RepresentationFacts {
            boundary: if minimum {
                RepresentationBoundary::MinimumUnit
            } else {
                RepresentationBoundary::SingleOutput
            },
            layout: RepresentationLayout {
                domain: domain::KERNEL,
                code: NonZeroU16::new(if self.text { 41 } else { 31 }).unwrap(),
            },
            required: u64::try_from(if minimum { self.largest } else { self.total }).ok(),
            extent_limit: Some(i32::MAX as u64),
            representable_prefix: None,
        })
    }
}
impl novarocks_failure_contract::OuterFailureProjection for BitmapOutputExtent {}
impl BitmapOutputTally {
    pub fn for_text() -> Self {
        Self {
            text: true,
            ..Self::default()
        }
    }
    pub fn append(&mut self, bytes: usize) -> Result<(), crate::KernelFailure> {
        self.total = self.total.checked_add(bytes as u128).ok_or_else(|| {
            crate::KernelFailure::from_domain(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?;
        self.largest = self.largest.max(bytes as u128);
        if self.total > i32::MAX as u128 {
            Err(crate::KernelFailure::from_domain(BitmapOutputExtent {
                total: self.total,
                largest: self.largest,
                text: self.text,
            }))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bitmap_value::{decode_bitmap, encode_internal_bitmap};
    use std::collections::BTreeSet;
    #[test]
    fn bitmap_data_and_entry_count_extent_are_authored_before_host_projection() {
        for values in [
            BTreeSet::new(),
            BTreeSet::from([1]),
            BTreeSet::from([u64::MAX]),
            BTreeSet::from([1, 300, u64::MAX]),
        ] {
            let bytes = encode_internal_bitmap(&values).unwrap();
            assert_eq!(decode_bitmap(&bytes).unwrap(), values);
        }
        let invalid = decode_bitmap(&[255, 255]).unwrap_err();
        assert!(matches!(invalid, BitmapValueFailure::InvalidEncoding(_)));
        assert_eq!(invalid.failure_fact().class(), FailureClass::Data);
        assert_eq!(bitmap_wire_count(u32::MAX as usize).unwrap(), u32::MAX);
        if let Some(entries) = (u32::MAX as usize).checked_add(1) {
            let extent = bitmap_wire_count(entries).unwrap_err();
            assert_eq!(extent.failure_fact().class(), FailureClass::Representation);
            let facts = extent.failure_fact().representation().unwrap();
            assert_eq!(facts.required, Some(u64::from(u32::MAX) + 1));
            assert_eq!(facts.extent_limit, Some(u64::from(u32::MAX)));
        }
    }
    #[test]
    fn aggregate_geometry_matches_actual_wire_and_refuses_missing_singleton_or_wide_output() {
        use crate::bitmap_value::{aggregate_bitmap_byte_len, encode_bitmap_aggregate};
        for values in [
            BTreeSet::new(),
            BTreeSet::from([7]),
            BTreeSet::from([u64::MAX]),
            BTreeSet::from([1, 300, u64::MAX]),
        ] {
            let old = crate::bitmap_value::encode_bitmap_aggregate(&values).unwrap();
            let typed = encode_bitmap_aggregate(&values).unwrap();
            assert_eq!(typed, old);
            assert_eq!(
                aggregate_bitmap_byte_len(values.len(), values.first().copied()).unwrap(),
                old.len()
            );
        }
        let missing = aggregate_bitmap_byte_len(1, None).unwrap_err();
        assert_eq!(missing.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(missing.failure_fact().reason().code(), 197);
        let Some(over_limit) = (i32::MAX as usize).checked_add(1) else {
            return;
        };
        let mut batch = BitmapOutputTally::default();
        batch.append(i32::MAX as usize).unwrap();
        let batch = batch.append(1).unwrap_err().failure_fact();
        let single = BitmapOutputTally::default()
            .append(over_limit)
            .unwrap_err()
            .failure_fact();
        for (fact, boundary) in [
            (batch, RepresentationBoundary::SingleOutput),
            (single, RepresentationBoundary::MinimumUnit),
        ] {
            assert_eq!(fact.class(), FailureClass::Representation);
            assert_eq!(fact.reason().code(), 196);
            let representation = fact.representation().unwrap();
            assert_eq!(representation.boundary, boundary);
            assert_eq!(representation.layout.code.get(), 31);
            assert_eq!(representation.required, Some(i32::MAX as u64 + 1));
        }
    }
}

#[cfg(test)]
mod source_tests {
    use super::*;
    #[test]
    fn bitmap_decode_only_actual_encoding_errors_are_data() {
        let malformed = BitmapValueFailure::DecodeIo(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "resource exhausted cancelled",
        ));
        assert!(malformed.is_data());
        assert_eq!(
            malformed.failure_fact(),
            crate::DataFailureReason::InvalidEncoding.failure_fact()
        );
        let allocation = BitmapValueFailure::DecodeIo(std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "invalid encoding",
        ));
        assert!(!allocation.is_data());
        assert_eq!(allocation.failure_fact().class(), FailureClass::Resource);
        assert!(allocation.source().unwrap().is::<std::io::Error>());
        let writer = BitmapValueFailure::EncodeIo(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad value",
        ));
        assert!(!writer.is_data());
        assert_eq!(writer.failure_fact().class(), FailureClass::Unexpected);
    }
    #[test]
    fn text_offsets_distinguish_one_oversized_value_from_full_output() {
        let mut output = BitmapOutputTally::for_text();
        output.append(i32::MAX as usize).unwrap();
        let failure = output.append(1).unwrap_err();
        let fact = failure.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        assert_eq!(
            fact.representation().unwrap().boundary,
            RepresentationBoundary::SingleOutput
        );
        let mut value = BitmapOutputTally::for_text();
        let failure = value.append(i32::MAX as usize + 1).unwrap_err();
        let fact = failure.failure_fact();
        assert_eq!(
            fact.representation().unwrap().boundary,
            RepresentationBoundary::MinimumUnit
        );
        assert_eq!(fact.representation().unwrap().layout.code.get(), 41);
    }
}
