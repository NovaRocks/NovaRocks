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

//! Domain-owned, transport-neutral failure facts.
//!
//! Producers project their own reasons. This crate knows neither runtime error
//! types nor Task lifecycle, recovery policy, SQL codes, or Arrow carriers.

use std::{fmt, num::NonZeroU16};

/// LocalCompiler-owned IDs shared only for their existing public consumers.
/// Their projection and meaning remain defined by the producing compiler.
pub mod local_compiler_reason;
/// The compiled-package encoder's original reason used by its public consumer.
pub mod plan_codec_reason;

/// Bound for retained diagnostics. Size and redaction are separate claims.
pub const DIAGNOSTIC_MAX_BYTES: usize = 512;

/// A bounded local rendering. It makes no redaction claim and is not itself a
/// protocol/status diagnostic. Boundaries must project safe facts explicitly.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BoundedDetail(String);
impl BoundedDetail {
    /// Exact allocation extent of the owned UTF-8 prefix made by truncating.
    /// Unlike Display formatting, this path performs one str::to_owned copy.
    pub fn truncating_backing_len(value: &str) -> usize {
        let mut end = value.len().min(DIAGNOSTIC_MAX_BYTES);
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        end
    }
    pub fn truncating(value: &str) -> Self {
        Self(value[..Self::truncating_backing_len(value)].to_owned())
    }
    pub fn truncating_display(value: impl fmt::Display) -> Self {
        struct Bounded(String);
        impl fmt::Write for Bounded {
            fn write_str(&mut self, value: &str) -> fmt::Result {
                let remaining = DIAGNOSTIC_MAX_BYTES - self.0.len();
                if value.len() <= remaining {
                    self.0.push_str(value);
                    return Ok(());
                }
                let mut end = remaining;
                while !value.is_char_boundary(end) {
                    end -= 1;
                }
                self.0.push_str(&value[..end]);
                Err(fmt::Error)
            }
        }
        let mut output = Bounded(String::new());
        let _ = fmt::write(&mut output, format_args!("{value}"));
        Self(output.0)
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn into_string(self) -> String {
        self.0
    }
}
impl fmt::Display for BoundedDetail {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Semantic category, independent of the component that produced the error.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum FailureClass {
    Data,
    Dependency,
    Resource,
    Representation,
    Invariant,
    Unexpected,
    Control,
    /// A valid request requires an explicitly unavailable implementation capability.
    Unsupported,
}

/// Stable producer namespace and producer-owned reason. Zero is never a reason.
/// Domain owners define constants; boundary codecs preserve both integers.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct FailureReason {
    domain: NonZeroU16,
    code: NonZeroU16,
}

impl FailureReason {
    pub const fn new(domain: NonZeroU16, code: NonZeroU16) -> Self {
        Self { domain, code }
    }
    pub const fn domain(self) -> u16 {
        self.domain.get()
    }
    pub const fn code(self) -> u16 {
        self.code.get()
    }
}

impl fmt::Display for FailureReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.domain, self.code)
    }
}

/// Stable namespaces. Each reason is defined by its owning producer domain.
/// Cross-boundary symbols retain that ownership when shared from this crate.
pub mod domain {
    use std::num::NonZeroU16;
    pub const KERNEL: NonZeroU16 = NonZeroU16::new(1).unwrap();
    pub const CONSTRUCTION: NonZeroU16 = NonZeroU16::new(2).unwrap();
    pub const MEMORY: NonZeroU16 = NonZeroU16::new(3).unwrap();
    pub const CONNECTOR: NonZeroU16 = NonZeroU16::new(4).unwrap();
    pub const EXCHANGE: NonZeroU16 = NonZeroU16::new(5).unwrap();
    pub const RESULT: NonZeroU16 = NonZeroU16::new(6).unwrap();
    pub const EXECUTION: NonZeroU16 = NonZeroU16::new(7).unwrap();
    pub const METADATA: NonZeroU16 = NonZeroU16::new(8).unwrap();
    pub const TYPE_CONTRACT: NonZeroU16 = NonZeroU16::new(9).unwrap();
    pub const WORKER: NonZeroU16 = NonZeroU16::new(10).unwrap();
    pub const SQL: NonZeroU16 = NonZeroU16::new(11).unwrap();
    pub const LOCAL_PROGRAM: NonZeroU16 = NonZeroU16::new(12).unwrap();
    pub const NATIVE: NonZeroU16 = NonZeroU16::new(13).unwrap();
    pub const FUNCTION_BINDING: NonZeroU16 = NonZeroU16::new(14).unwrap();
    pub const PROTOCOL: NonZeroU16 = NonZeroU16::new(15).unwrap();
    pub const CONSTANT: NonZeroU16 = NonZeroU16::new(16).unwrap();
    pub const PLAN_CODEC: NonZeroU16 = NonZeroU16::new(17).unwrap();
    pub const PHYSICAL_PLAN: NonZeroU16 = NonZeroU16::new(18).unwrap();
    pub const LOCAL_COMPILER: NonZeroU16 = NonZeroU16::new(19).unwrap();
    pub const QUERY: NonZeroU16 = NonZeroU16::new(20).unwrap();
    pub const WORKLOAD: NonZeroU16 = NonZeroU16::new(21).unwrap();
    pub const VALUE_ARRAYS: NonZeroU16 = NonZeroU16::new(22).unwrap();
    pub const STORAGE_AUTHORITY: NonZeroU16 = NonZeroU16::new(23).unwrap();
    pub const STATE_STORE: NonZeroU16 = NonZeroU16::new(24).unwrap();
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ResourceScope {
    Request,
    Operator,
    Driver,
    Task,
    Query,
    Process,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ResourceDimension {
    Bytes,
    Items,
    Positions,
}

/// Unknown measurements remain None; zero remains an actual observed zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceFacts {
    pub request_scope: Option<ResourceScope>,
    pub constraint_scope: Option<ResourceScope>,
    pub dimension: ResourceDimension,
    pub requested: Option<u64>,
    pub required_growth: Option<u64>,
    pub limit: Option<u64>,
    pub live: Option<u64>,
    pub committed: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepresentationBoundary {
    MinimumUnit,
    SingleOutput,
}

/// A domain's stable layout identifier, distinct from an error reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepresentationLayout {
    pub domain: NonZeroU16,
    pub code: NonZeroU16,
}

/// Layout identity belongs to the producing domain. The contract carries its
/// stable reason and numerical boundary without importing a value crate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepresentationFacts {
    pub boundary: RepresentationBoundary,
    pub layout: RepresentationLayout,
    /// Required extent in the units defined by this domain's layout.
    pub required: Option<u64>,
    /// The actual format/API extent limit in the same units as `required`.
    /// This is not a number of deliverable rows or an allocation grant.
    pub extent_limit: Option<u64>,
    /// Number of leading input rows actually proved representable for this
    /// failed request. Neither a format limit nor MinimumUnit alone proves
    /// a whole-input row prefix. Without that independent proof, use None.
    pub representable_prefix: Option<u64>,
}

/// Causal evidence belongs to the producing domain, independently of class.
/// A local timeout can originate a Control failure; observing somebody else's
/// accepted stop cannot replace that cause or refine a peer placeholder.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureEvidence {
    Originating,
    StopObservation,
}

/// Exact data position once the required-root owner has resolved selection.
/// A lower-level journal alone cannot invent its canonical batch position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FailureRowPosition {
    pub selected_ordinal: u64,
    pub batch_row: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FailureFact {
    class: FailureClass,
    evidence: FailureEvidence,
    row_position: Option<FailureRowPosition>,
    reason: FailureReason,
    resource: Option<ResourceFacts>,
    representation: Option<RepresentationFacts>,
}

impl FailureFact {
    pub const fn new(class: FailureClass, reason: FailureReason) -> Self {
        Self {
            class,
            evidence: FailureEvidence::Originating,
            row_position: None,
            reason,
            resource: None,
            representation: None,
        }
    }
    pub const fn stop_observation(reason: FailureReason) -> Self {
        Self {
            evidence: FailureEvidence::StopObservation,
            ..Self::new(FailureClass::Control, reason)
        }
    }
    pub const fn with_row_position(mut self, position: FailureRowPosition) -> Self {
        self.row_position = Some(position);
        self
    }
    pub const fn row_position(self) -> Option<FailureRowPosition> {
        self.row_position
    }
    pub const fn evidence(self) -> FailureEvidence {
        self.evidence
    }
    pub const fn with_resource(mut self, resource: ResourceFacts) -> Self {
        self.resource = Some(resource);
        self
    }
    pub const fn with_representation(mut self, representation: RepresentationFacts) -> Self {
        self.representation = Some(representation);
        self
    }
    pub const fn class(self) -> FailureClass {
        self.class
    }
    pub const fn reason(self) -> FailureReason {
        self.reason
    }
    pub const fn resource(self) -> Option<ResourceFacts> {
        self.resource
    }
    pub const fn representation(self) -> Option<RepresentationFacts> {
        self.representation
    }
}

/// Implemented where the reason is known. Wrappers delegate this projection;
/// they never classify another domain's concrete error type or its Display.
// Design: ADR-0187 (docs/adr/ADR-0187-receiver-owned-steps-and-single-task-conclusion.md)
pub trait FailureProjection {
    fn failure_fact(&self) -> FailureFact;
}

impl FailureProjection for std::convert::Infallible {
    fn failure_fact(&self) -> FailureFact {
        match *self {}
    }
}

impl<T: FailureProjection + ?Sized> FailureProjection for &T {
    fn failure_fact(&self) -> FailureFact {
        (**self).failure_fact()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // A domain can project a new cause without adding a runtime dependency or
    // extending a central match. Its own identifiers are the independent oracle.
    struct LocalCapacityRefusal;
    impl FailureProjection for LocalCapacityRefusal {
        fn failure_fact(&self) -> FailureFact {
            FailureFact::new(
                FailureClass::Resource,
                FailureReason::new(domain::MEMORY, NonZeroU16::new(41).unwrap()),
            )
            .with_resource(ResourceFacts {
                request_scope: Some(ResourceScope::Operator),
                constraint_scope: Some(ResourceScope::Query),
                dimension: ResourceDimension::Bytes,
                requested: Some(64),
                required_growth: None,
                limit: Some(32),
                live: None,
                committed: Some(0),
            })
        }
    }
    #[test]
    fn domain_projection_preserves_reason_and_unknown_measurements() {
        let fact = LocalCapacityRefusal.failure_fact();
        assert_eq!((fact.reason().domain(), fact.reason().code()), (3, 41));
        assert_eq!(fact.class(), FailureClass::Resource);
        let resource = fact.resource().unwrap();
        assert_eq!((resource.requested, resource.limit), (Some(64), Some(32)));
        assert_eq!((resource.live, resource.committed), (None, Some(0)));
    }
    #[test]
    fn representation_distinguishes_a_value_from_a_whole_output_boundary() {
        let layout = RepresentationLayout {
            domain: domain::CONSTRUCTION,
            code: NonZeroU16::new(2).unwrap(),
        };
        let one = RepresentationFacts {
            boundary: RepresentationBoundary::MinimumUnit,
            layout,
            required: Some(10),
            extent_limit: Some(8),
            representable_prefix: Some(0),
        };
        let batch = RepresentationFacts {
            boundary: RepresentationBoundary::SingleOutput,
            layout,
            required: Some(10),
            extent_limit: Some(8),
            representable_prefix: Some(3),
        };
        assert_ne!(one, batch);
        assert_eq!(one.extent_limit, batch.extent_limit);
        assert_eq!(one.representable_prefix, Some(0));
        assert_eq!(batch.representable_prefix, Some(3));
    }
}

mod shared_cause;
pub use shared_cause::SharedFailureCause;

/// A producer declares an unmaskable outer failure channel. Per-row data
/// errors do not implement this marker and keep their separate row channel.
/// This supplies no cancellation, retry or capacity authority.
pub trait OuterFailureProjection:
    FailureProjection + std::error::Error + Send + Sync + 'static
{
}

#[cfg(test)]
mod evidence_tests {
    use super::*;
    #[test]
    fn control_category_alone_does_not_determine_causality() {
        let reason = FailureReason::new(domain::EXECUTION, NonZeroU16::new(65000).unwrap());
        let local_timeout = FailureFact::new(FailureClass::Control, reason);
        let observed = FailureFact::stop_observation(reason);
        assert_eq!(local_timeout.class(), observed.class());
        assert_eq!(local_timeout.reason(), observed.reason());
        assert_eq!(local_timeout.evidence(), FailureEvidence::Originating);
        assert_eq!(observed.evidence(), FailureEvidence::StopObservation);
    }
}
