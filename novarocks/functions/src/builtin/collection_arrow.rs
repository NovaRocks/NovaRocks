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
//! Shared collection Arrow operations and their own unmaskable domain failures.
//! Default-safe casts keep their original NULL rules; an SDK error is not a
//! row journal entry. This owner grants no capacity or allocation authority.
use crate::KernelFailure;
use arrow_array::ArrayRef;
use arrow_schema::{ArrowError, DataType};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, FailureReason,
    RepresentationBoundary, RepresentationFacts, RepresentationLayout, domain,
};

pub fn cast_output(
    out: ArrayRef,
    output_type: Option<&DataType>,
    label: &str,
) -> Result<ArrayRef, KernelFailure> {
    let Some(target) = output_type else {
        return Ok(out);
    };
    if out.data_type() == target {
        return Ok(out);
    }
    arrow_cast::cast(&out, target).map_err(|source| arrow_failure(label, ArrowSite::Cast, source))
}
#[derive(Clone, Copy, Debug)]
pub(super) enum ArrowSite {
    Cast,
    Take,
    Construct,
    Extend,
}
#[derive(Debug)]
enum CollectionArrowFailure {
    Contract(BoundedDetail),
    OffsetEntries {
        required: u128,
        source: Option<std::alloc::LayoutError>,
    },
    Arrow {
        label: BoundedDetail,
        site: ArrowSite,
        source: ArrowError,
    },
}
impl std::fmt::Display for CollectionArrowFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) => detail.fmt(f),
            Self::OffsetEntries { required, .. } => write!(
                f,
                "collection output requires {required} list offset entries"
            ),
            Self::Arrow {
                label,
                site,
                source,
            } => write!(
                f,
                "{label}: {}: {source}",
                match site {
                    ArrowSite::Cast => "failed to cast output",
                    ArrowSite::Take => "take failed",
                    ArrowSite::Construct => "array construction failed",
                    ArrowSite::Extend => "array extension failed",
                }
            ),
        }
    }
}
impl std::error::Error for CollectionArrowFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Contract(_) | Self::OffsetEntries { source: None, .. } => None,
            Self::OffsetEntries {
                source: Some(source),
                ..
            } => Some(source),
            Self::Arrow { source, .. } => Some(source),
        }
    }
}
impl FailureProjection for CollectionArrowFailure {
    fn failure_fact(&self) -> FailureFact {
        if let Self::OffsetEntries { required, .. } = self {
            let code = std::num::NonZeroU16::new(388).unwrap();
            return FailureFact::new(
                FailureClass::Representation,
                FailureReason::new(domain::KERNEL, code),
            )
            .with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code,
                },
                required: u64::try_from(*required).ok(),
                extent_limit: u64::try_from(isize::MAX as usize / std::mem::size_of::<i32>()).ok(),
                representable_prefix: None,
            });
        }
        let (class, code) = match self {
            Self::OffsetEntries { .. } => unreachable!("handled above"),
            Self::Contract(_) => (FailureClass::Invariant, 374),
            Self::Arrow {
                source: ArrowError::MemoryError(_),
                ..
            } => (FailureClass::Resource, 376),
            Self::Arrow { .. } => (FailureClass::Invariant, 375),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for CollectionArrowFailure {}
pub(super) fn contract(detail: impl std::fmt::Display) -> KernelFailure {
    KernelFailure::from_domain(CollectionArrowFailure::Contract(
        BoundedDetail::truncating_display(detail),
    ))
}
pub(super) fn arrow_failure(label: &str, site: ArrowSite, source: ArrowError) -> KernelFailure {
    KernelFailure::from_domain(CollectionArrowFailure::Arrow {
        label: BoundedDetail::truncating(label),
        site,
        source,
    })
}

/// Only an actual extension failure owns this completion priority. Other
/// collection contract failures keep their existing caller completion rules.
pub(super) fn is_extension_failure(failure: &KernelFailure) -> bool {
    let mut source = std::error::Error::source(failure);
    while let Some(current) = source {
        if let Some(failure) = current.downcast_ref::<CollectionArrowFailure>() {
            return matches!(
                failure,
                CollectionArrowFailure::Arrow {
                    site: ArrowSite::Extend,
                    ..
                }
            );
        }
        source = current.source();
    }
    false
}

/// Check the actual List offset-header geometry before allocating it. Entries
/// are the format unit; this establishes no allocator capacity or memory grant.
pub(super) fn list_offset_entries(rows: usize) -> Result<usize, KernelFailure> {
    let required = rows as u128 + 1;
    if required > usize::MAX as u128 {
        return Err(KernelFailure::from_domain(
            CollectionArrowFailure::OffsetEntries {
                required,
                source: None,
            },
        ));
    }
    let count = required as usize;
    std::alloc::Layout::array::<i32>(count).map_err(|source| {
        KernelFailure::from_domain(CollectionArrowFailure::OffsetEntries {
            required,
            source: Some(source),
        })
    })?;
    Ok(count)
}
