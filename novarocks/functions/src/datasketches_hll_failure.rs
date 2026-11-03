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

//! Owned DS HLL failures at their actual construction frontier.
//! Library errors retain their original identity; payload bytes are never kept.
use std::fmt;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HllDiagnosticContext {
    Direct,
    InitialPreflight,
    MergePreflight,
}
impl fmt::Display for HllDiagnosticContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Direct => "ds_hll",
            Self::InitialPreflight => "ds_hll preflight",
            Self::MergePreflight => "ds_hll merge preflight",
        })
    }
}
#[derive(Debug)]
pub enum HllFailureDetail {
    Truncated(usize),
    Length { required: usize, actual: usize },
    Version(u8),
    Family(u8),
    LogK(u8),
    Target(u8),
    ListHeader { preamble: u8, lg_arr: u8 },
    ListCount,
    SetPreamble(u8),
    SetLgArr { max: u8, actual: u8 },
    SetCount { count: usize, capacity: usize },
    HllPreamble(u8),
    RegisterCount,
    Hll4LgArr(u8),
    AuxOverflow { slots: usize },
    PayloadOverflow { packed: usize, auxiliary: usize },
    Hll6Aux,
    Hll8Aux,
    Mode(u8),
    Deserialize(datasketches::error::Error),
    Create(datasketches::error::Error),
    NewLogK(u8),
}
#[derive(Debug)]
pub struct HllFailureRecipe {
    pub context: HllDiagnosticContext,
    pub detail: HllFailureDetail,
}
impl fmt::Display for HllFailureRecipe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use HllFailureDetail::*;
        let context = self.context;
        match &self.detail {
            Truncated(offset) => write!(f, "{context}: HLL header is truncated at byte {offset}"),
            Length { required, actual } => write!(
                f,
                "{context}: HLL payload requires {required} bytes, got {actual}"
            ),
            Version(actual) => write!(
                f,
                "{context}: expected HLL serial version {}, got {actual}",
                crate::datasketches_hll::HLL_SERIAL_VERSION
            ),
            Family(actual) => write!(
                f,
                "{context}: expected HLL family {}, got {actual}",
                crate::datasketches_hll::HLL_FAMILY_ID
            ),
            LogK(actual) => write!(f, "{context}: HLL lg_k must be in [4, 21], got {actual}"),
            Target(actual) => write!(f, "{context}: invalid HLL target type {actual}"),
            ListHeader { preamble, lg_arr } => write!(
                f,
                "{context}: invalid LIST header preamble={preamble} lg_arr={lg_arr}"
            ),
            ListCount => write!(f, "{context}: invalid LIST coupon count or empty flag"),
            SetPreamble(actual) => write!(f, "{context}: invalid SET preamble {actual}"),
            SetLgArr { max, actual } => write!(
                f,
                "{context}: SET lg_arr must be in [5, {max}], got {actual}"
            ),
            SetCount { count, capacity } => write!(
                f,
                "{context}: SET coupon count {count} exceeds capacity {capacity}"
            ),
            HllPreamble(actual) => write!(f, "{context}: invalid HLL preamble {actual}"),
            RegisterCount => write!(f, "{context}: HLL register or auxiliary count exceeds k"),
            Hll4LgArr(actual) => write!(f, "{context}: invalid HLL4 lg_arr {actual}"),
            AuxOverflow { .. } => write!(f, "{context}: HLL4 auxiliary payload length overflows"),
            PayloadOverflow { .. } => write!(f, "{context}: HLL4 payload length overflows"),
            Hll6Aux => write!(f, "{context}: HLL6 auxiliary count must be zero"),
            Hll8Aux => write!(f, "{context}: HLL8 auxiliary count must be zero"),
            Mode(actual) => write!(f, "{context}: invalid HLL mode {actual}"),
            Deserialize(error) => {
                write!(f, "{context}: failed to deserialize HLL payload: {error}")
            }
            Create(error) => write!(f, "ds_hll: failed to create HLL union: {error}"),
            NewLogK(actual) => write!(f, "ds_hll log_k must be in [4, 21], got {actual}"),
        }
    }
}
impl std::error::Error for HllFailureRecipe {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.detail {
            HllFailureDetail::Deserialize(source) | HllFailureDetail::Create(source) => {
                Some(source)
            }
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for HllFailureRecipe {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, ResourceDimension, ResourceFacts, domain,
        };
        use std::num::NonZeroU16;
        let (class, code) = match self.detail {
            HllFailureDetail::Deserialize(_) => {
                return crate::DataFailureReason::InvalidEncoding.failure_fact();
            }
            // Construction follows validated immutable log_k/target preflight.
            HllFailureDetail::Create(_) => (FailureClass::Invariant, 179),
            HllFailureDetail::NewLogK(_) => {
                return crate::DataFailureReason::NumericOutOfRange.failure_fact();
            }
            HllFailureDetail::AuxOverflow { slots } => {
                return FailureFact::new(
                    FailureClass::Resource,
                    FailureReason::new(domain::KERNEL, NonZeroU16::new(181).unwrap()),
                )
                .with_resource(ResourceFacts {
                    request_scope: None,
                    constraint_scope: None,
                    dimension: ResourceDimension::Bytes,
                    requested: u64::try_from(slots as u128 * 4).ok(),
                    required_growth: None,
                    limit: u64::try_from(usize::MAX).ok(),
                    live: None,
                    committed: None,
                });
            }
            HllFailureDetail::PayloadOverflow { packed, auxiliary } => {
                return FailureFact::new(
                    FailureClass::Resource,
                    FailureReason::new(domain::KERNEL, NonZeroU16::new(181).unwrap()),
                )
                .with_resource(ResourceFacts {
                    request_scope: None,
                    constraint_scope: None,
                    dimension: ResourceDimension::Bytes,
                    requested: u64::try_from(packed as u128 + auxiliary as u128).ok(),
                    required_growth: None,
                    limit: u64::try_from(usize::MAX).ok(),
                    live: None,
                    committed: None,
                });
            }
            _ => return crate::DataFailureReason::InvalidEncoding.failure_fact(),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::KERNEL, NonZeroU16::new(code).unwrap()),
        )
    }
}
/// The owning producer separates actual value failures before a caller can
/// construct an outer kernel error. Neither wrapper has a public constructor.
#[derive(Debug)]
pub enum ClassifiedHllFailure {
    Value(HllValueFailure),
    Outer(HllOuterFailure),
}
/// A value failure cannot enter a selected kernel's outer error channel.
/// ```compile_fail
/// use novarocks_functions::datasketches_hll_failure::{HllFailureRecipe, HllFailureDetail,
///     HllDiagnosticContext, ClassifiedHllFailure};
/// let cause = HllFailureRecipe { context: HllDiagnosticContext::Direct,
///     detail: HllFailureDetail::Length { required: 8, actual: 3 } };
/// if let ClassifiedHllFailure::Value(value) = cause.classify() {
///     let _ = novarocks_functions::KernelFailure::from_domain(value);
/// }
/// ```
#[derive(Debug)]
pub struct HllValueFailure(HllFailureRecipe);
#[derive(Debug)]
pub struct HllOuterFailure(HllOuterCause);
#[derive(Debug)]
enum HllOuterCause {
    Recipe(HllFailureRecipe),
    Invariant(HllInvariantRecipe),
}
impl From<HllInvariantRecipe> for ClassifiedHllFailure {
    fn from(source: HllInvariantRecipe) -> Self {
        Self::Outer(HllOuterFailure(HllOuterCause::Invariant(source)))
    }
}
impl fmt::Display for ClassifiedHllFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Value(source) => source.fmt(f),
            Self::Outer(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for ClassifiedHllFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Value(source) => Some(source),
            Self::Outer(source) => Some(source),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ClassifiedHllFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::Value(source) => source.failure_fact(),
            Self::Outer(source) => source.failure_fact(),
        }
    }
}
impl HllFailureRecipe {
    pub fn classify(self) -> ClassifiedHllFailure {
        match &self.detail {
            HllFailureDetail::Create(_)
            | HllFailureDetail::AuxOverflow { .. }
            | HllFailureDetail::PayloadOverflow { .. } => {
                ClassifiedHllFailure::Outer(HllOuterFailure(HllOuterCause::Recipe(self)))
            }
            _ => ClassifiedHllFailure::Value(HllValueFailure(self)),
        }
    }
}
impl HllValueFailure {
    pub fn reason(&self) -> crate::DataFailureReason {
        match self.0.detail {
            HllFailureDetail::NewLogK(_) => crate::DataFailureReason::NumericOutOfRange,
            _ => crate::DataFailureReason::InvalidEncoding,
        }
    }
}
impl fmt::Display for HllValueFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl fmt::Display for HllOuterFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            HllOuterCause::Recipe(source) => source.fmt(f),
            HllOuterCause::Invariant(source) => source.fmt(f),
        }
    }
}
impl std::error::Error for HllValueFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
impl std::error::Error for HllOuterFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.0 {
            HllOuterCause::Recipe(source) => Some(source),
            HllOuterCause::Invariant(source) => Some(source),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for HllValueFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        self.0.failure_fact()
    }
}
impl novarocks_failure_contract::FailureProjection for HllOuterFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match &self.0 {
            HllOuterCause::Recipe(source) => source.failure_fact(),
            HllOuterCause::Invariant(source) => source.failure_fact(),
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for HllOuterFailure {}

#[derive(Clone, Copy, Debug)]
pub enum HllInvariantRecipe {
    NewPreflight,
    PayloadPreflight,
    UpdatePreflight,
    MergePreflight,
    SerializePreflight,
    Generation,
}
impl HllInvariantRecipe {
    pub const fn message(self) -> &'static str {
        match self {
            Self::NewPreflight => "ds_hll: new allocation preflight does not match operation",
            Self::PayloadPreflight => {
                "ds_hll: payload allocation preflight does not match operation"
            }
            Self::UpdatePreflight => "ds_hll: update allocation preflight is stale or mismatched",
            Self::MergePreflight => "ds_hll: merge allocation preflight is stale or mismatched",
            Self::SerializePreflight => {
                "ds_hll: serialize allocation preflight is stale or mismatched"
            }
            Self::Generation => "ds_hll: accumulator generation overflow",
        }
    }
}
impl fmt::Display for HllInvariantRecipe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

impl std::error::Error for HllInvariantRecipe {}
impl novarocks_failure_contract::FailureProjection for HllInvariantRecipe {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, FailureReason, ResourceDimension, ResourceFacts, domain,
        };
        use std::num::NonZeroU16;
        if matches!(self, Self::Generation) {
            FailureFact::new(
                FailureClass::Resource,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(183).unwrap()),
            )
            .with_resource(ResourceFacts {
                request_scope: None,
                constraint_scope: None,
                dimension: ResourceDimension::Items,
                requested: None,
                required_growth: Some(1),
                limit: Some(u64::MAX),
                live: None,
                committed: Some(u64::MAX),
            })
        } else {
            FailureFact::new(
                FailureClass::Invariant,
                FailureReason::new(domain::KERNEL, NonZeroU16::new(182).unwrap()),
            )
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for HllInvariantRecipe {}

#[derive(Clone, Copy, Debug)]
pub enum HllObservation {
    Step,
    OpaqueBoundary,
}
pub trait HllFailureSink {
    type Error;
    fn failure(&mut self, recipe: HllFailureRecipe) -> Self::Error;
    fn invariant(&mut self, recipe: HllInvariantRecipe) -> Self::Error;
    fn observe(&mut self, event: HllObservation) -> Result<(), Self::Error>;
}
pub struct RawHllFailure;
impl HllFailureSink for RawHllFailure {
    type Error = ClassifiedHllFailure;
    fn failure(&mut self, recipe: HllFailureRecipe) -> Self::Error {
        recipe.classify()
    }
    fn invariant(&mut self, recipe: HllInvariantRecipe) -> Self::Error {
        recipe.into()
    }
    fn observe(&mut self, _: HllObservation) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[cfg(test)]
mod owned_failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::{error::Error, sync::Arc};
    #[test]
    fn actual_library_decode_error_keeps_its_type_without_retaining_the_payload_owner() {
        let payload = Arc::new(vec![0_u8; 3]);
        let weak = Arc::downgrade(&payload);
        let source = datasketches::hll::HllSketch::deserialize(&payload).unwrap_err();
        let kind = source.kind();
        let error = HllFailureRecipe {
            context: HllDiagnosticContext::Direct,
            detail: HllFailureDetail::Deserialize(source),
        };
        let source = error
            .source()
            .unwrap()
            .downcast_ref::<datasketches::error::Error>()
            .unwrap();
        assert_eq!(source.kind(), kind);
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert_eq!(
            error.failure_fact(),
            crate::DataFailureReason::InvalidEncoding.failure_fact()
        );
        let ClassifiedHllFailure::Value(failure) = error.classify() else {
            panic!("actual decode value cause")
        };
        drop(payload);
        assert!(weak.upgrade().is_none());
        assert_eq!(
            failure.failure_fact(),
            crate::DataFailureReason::InvalidEncoding.failure_fact()
        );
        let original = &failure;
        let original = original
            .source()
            .unwrap()
            .downcast_ref::<HllFailureRecipe>()
            .unwrap();
        assert_eq!(
            original
                .source()
                .unwrap()
                .downcast_ref::<datasketches::error::Error>()
                .unwrap()
                .kind(),
            kind
        );
    }
    #[test]
    fn malformed_payload_and_actual_storage_geometry_are_distinct() {
        let error = HllFailureRecipe {
            context: HllDiagnosticContext::InitialPreflight,
            detail: HllFailureDetail::Length {
                required: 8,
                actual: 3,
            },
        };
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        let error = HllFailureRecipe {
            context: HllDiagnosticContext::MergePreflight,
            detail: HllFailureDetail::PayloadOverflow {
                packed: usize::MAX,
                auxiliary: 1,
            },
        };
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        let resource = fact.resource().unwrap();
        assert_eq!(
            resource.requested,
            u64::try_from(usize::MAX as u128 + 1).ok()
        );
        assert_eq!(resource.limit, u64::try_from(usize::MAX).ok());
        assert_eq!((resource.live, resource.committed), (None, None));
        assert_eq!(
            HllInvariantRecipe::UpdatePreflight.failure_fact().class(),
            FailureClass::Invariant
        );
        let fact = HllInvariantRecipe::Generation.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.resource().unwrap().required_growth, Some(1));
        assert_eq!(fact.resource().unwrap().committed, Some(u64::MAX));
    }
}
